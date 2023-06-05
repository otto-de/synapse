package de.otto.synapse.channel;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.receiver.AbstractMessageLogReceiverEndpoint;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpoint;
import de.otto.synapse.endpoint.receiver.MessageQueueReceiverEndpoint;
import de.otto.synapse.message.Header;
import de.otto.synapse.message.TextMessage;
import jakarta.annotation.Nonnull;
import org.slf4j.Logger;
import org.springframework.context.ApplicationEventPublisher;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import static com.google.common.collect.Iterables.getLast;
import static de.otto.synapse.channel.ChannelDurationBehind.channelDurationBehind;
import static de.otto.synapse.channel.ChannelPosition.channelPosition;
import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static de.otto.synapse.channel.ShardResponse.shardResponse;
import static de.otto.synapse.channel.StartFrom.HORIZON;
import static de.otto.synapse.info.MessageReceiverStatus.*;
import static java.time.Duration.ZERO;
import static java.time.Duration.between;
import static java.time.Instant.now;
import static java.util.Collections.synchronizedList;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.slf4j.LoggerFactory.getLogger;

public class InMemoryChannel extends AbstractMessageLogReceiverEndpoint implements MessageLogReceiverEndpoint, MessageQueueReceiverEndpoint {

    private static final Logger LOG = getLogger(InMemoryChannel.class);
    private final List<TextMessage> eventQueue;
    private final AtomicBoolean stopSignal = new AtomicBoolean(false);

    public InMemoryChannel(final String channelName,
                           final MessageInterceptorRegistry interceptorRegistry) {
        super(channelName, interceptorRegistry, null);
        this.eventQueue = synchronizedList(new ArrayList<>());
    }

    public InMemoryChannel(final String channelName,
                           final MessageInterceptorRegistry interceptorRegistry,
                           final ApplicationEventPublisher eventPublisher) {
        super(channelName, interceptorRegistry, eventPublisher);
        this.eventQueue = synchronizedList(new ArrayList<>());
    }

    public List<TextMessage> getEventQueue() {
        return eventQueue;
    }

    public synchronized void send(final TextMessage message) {
        final int position = eventQueue.size();
        LOG.info("Sending {} to {} at position{}", message, getChannelName(), position);
        final ImmutableMap<String, String> attributes = ImmutableMap.<String, String>builder()
                .putAll(message.getHeader().getAll())
                .put("synapse_msg_arrival_ts", now().toString())
                .build();
        eventQueue.add(TextMessage.of(
                message.getKey(),
                Header.of(
                        fromPosition(getChannelName(), String.valueOf(position)),
                        attributes),
                message.getPayload()));
    }

    @Nonnull
    public CompletableFuture<ChannelPosition> consumeUntil(@Nonnull final ChannelPosition startFrom,
                                                           @Nonnull final Predicate<ShardResponse> stopCondition) {
        publishEvent(STARTING, "Starting InMemoryChannel " + getChannelName(), null);
        publishEvent(STARTED, "Started InMemoryChannel " + getChannelName(), null);
        return CompletableFuture.supplyAsync(() -> {
            boolean shouldStop;
            ShardPosition shardPosition = startFrom.shard(getChannelName());
            AtomicInteger pos = new AtomicInteger(positionOf(shardPosition));
            do {
                final ImmutableList<TextMessage> messages;
                if (hasMessageAfter(pos.get())) {
                    final int index = pos.incrementAndGet();
                    final TextMessage receivedMessage = eventQueue.get(index);
                    messages = ImmutableList.of(receivedMessage);
                    LOG.info("Received message from channel={} at position={}: message={}", getChannelName(), index, receivedMessage);
                    final TextMessage interceptedMessage = intercept(receivedMessage);
                    if (interceptedMessage != null) {
                        try {
                            getMessageDispatcher().accept(interceptedMessage);
                        } catch (final Exception e) {
                            LOG.error("Error processing message: " + e.getMessage(), e);
                        }
                    }
                } else {
                    messages = ImmutableList.of();
                    try {
                        Thread.sleep(100);
                    } catch (final InterruptedException e) {
                        /* ignore */
                    }
                }
                shardPosition = fromPosition(getChannelName(), String.valueOf(pos));
                shouldStop = stopCondition.test(shardResponse(shardPosition, durationBehind(pos.get()), messages));
            } while (!shouldStop && !stopSignal.get());
            publishEvent(FINISHED, "Finished InMemoryChannel " + getChannelName(), null);
            return channelPosition(shardPosition);
        }, newSingleThreadExecutor());
    }

    private int positionOf(ShardPosition shardPosition) {
        return shardPosition.startFrom() == HORIZON
                ? -1
                : Integer.valueOf(shardPosition.position());
    }

    private Duration durationBehind(final int currentPos) {
        if (currentPos == -1 && eventQueue.size() > 0) {
            return Duration.between(
                    getLast(eventQueue).getHeader().getAsInstant("synapse_msg_arrival_ts"),
                    eventQueue.get(eventQueue.size()-1).getHeader().getAsInstant("synapse_msg_arrival_ts"))
                    .abs();
        } else if (currentPos >= 0 && currentPos <= eventQueue.size()) {
            return Duration.between(
                    getLast(eventQueue).getHeader().getAsInstant("synapse_msg_arrival_ts"),
                    eventQueue.get(currentPos).getHeader().getAsInstant("synapse_msg_arrival_ts"))
                    .abs();
        } else {
            return ZERO;
        }
    }

    @Override
    public CompletableFuture<Void> consume() {
        publishEvent(STARTING, "Starting InMemoryChannel " + getChannelName(), null);
        final TextMessage lastMessage = eventQueue.isEmpty() ? null : getLast(eventQueue);
        final ChannelDurationBehind durationBehind = lastMessage != null
                ? channelDurationBehind().with(getChannelName(), between(lastMessage.getHeader().getAsInstant("synapse_msg_arrival_ts"), now())).build()
                : null;
        publishEvent(STARTED, "Started InMemoryChannel " + getChannelName(), durationBehind);
        return CompletableFuture.supplyAsync(() -> {
            do {
                if (!eventQueue.isEmpty()) {
                    final TextMessage receivedMessage = eventQueue.remove(0);
                    final TextMessage interceptedMessage = intercept(
                            TextMessage.of(
                                    receivedMessage.getKey(),
                                    Header.of(null, receivedMessage.getHeader().getAll()),
                                    receivedMessage.getPayload()
                            )
                    );
                    if (interceptedMessage != null) {
                        try {
                            getMessageDispatcher().accept(interceptedMessage);
                        } catch (final Exception e) {
                            LOG.error("Error processing message: " + e.getMessage(), e);
                        }
                    }
                } else {
                    try {
                        Thread.sleep(100);
                    } catch (final InterruptedException e) {
                        /* ignore */
                    }
                }
            } while (!stopSignal.get());
            publishEvent(FINISHED, "Finished InMemoryChannel " + getChannelName(), durationBehind);
            return null;
        }, Executors.newSingleThreadExecutor());
    }

    @Override
    public void stop() {
        stopSignal.set(true);
    }

    private synchronized boolean hasMessageAfter(final int pos) {
        return eventQueue.size() > (pos+1);
    }
}
