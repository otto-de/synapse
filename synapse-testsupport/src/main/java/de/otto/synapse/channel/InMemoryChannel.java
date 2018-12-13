package de.otto.synapse.channel;

import com.google.common.collect.Iterables;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.receiver.AbstractMessageLogReceiverEndpoint;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpoint;
import de.otto.synapse.endpoint.receiver.MessageQueueReceiverEndpoint;
import de.otto.synapse.info.MessageReceiverStatus;
import de.otto.synapse.message.Message;
import org.slf4j.Logger;
import org.springframework.context.ApplicationEventPublisher;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static de.otto.synapse.channel.ChannelPosition.channelPosition;
import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static de.otto.synapse.message.Header.responseHeader;
import static de.otto.synapse.message.Message.message;
import static java.time.Duration.between;
import static java.time.Instant.now;
import static java.util.Collections.synchronizedList;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.slf4j.LoggerFactory.getLogger;

public class InMemoryChannel extends AbstractMessageLogReceiverEndpoint implements MessageLogReceiverEndpoint, MessageQueueReceiverEndpoint {

    private static final Logger LOG = getLogger(InMemoryChannel.class);
    private final List<Message<String>> eventQueue;
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

    public synchronized void send(final Message<String> message) {
        final int position = eventQueue.size();
        LOG.info("Sending {} to {} at position{}", message, getChannelName(), position);
        eventQueue.add(Message.message(
                message.getKey(),
                responseHeader(
                        fromPosition(getChannelName(), String.valueOf(position)),
                        Instant.now(),
                        message.getHeader().getAttributes()),
                message.getPayload()));
    }

    @Nonnull
    @Override
    public CompletableFuture<ChannelPosition> consumeUntil(@Nonnull final ChannelPosition startFrom,
                                                           @Nonnull final Instant until) {
        return consumeUntil(startFrom, (shardPosition) -> until.isAfter(now()) ) ;
    }

    @Nonnull
    @Override
    public CompletableFuture<ChannelPosition> catchUp(@Nonnull ChannelPosition startFrom) {
        return consumeUntil(startFrom, (shardPosition) -> {
            AtomicInteger pos = new AtomicInteger(shardPosition.startFrom() == StartFrom.HORIZON
                    ? -1
                    : Integer.valueOf(shardPosition.position()));
            return hasMessageAfter(pos.get());
        });
    }

    @Nonnull
    private CompletableFuture<ChannelPosition> consumeUntil(@Nonnull final ChannelPosition startFrom,
                                                           @Nonnull final Predicate<ShardPosition> stopCondition) {
        publishEvent(MessageReceiverStatus.STARTING, "Starting InMemoryChannel " + getChannelName(), null);
        final Message<String> lastMessage = eventQueue.isEmpty() ? null : Iterables.getLast(eventQueue);
        final ChannelDurationBehind durationBehind = lastMessage != null
                ? ChannelDurationBehind.channelDurationBehind().with(getChannelName(), between(lastMessage.getHeader().getArrivalTimestamp(), now())).build()
                : null;
        publishEvent(MessageReceiverStatus.STARTED, "Started InMemoryChannel " + getChannelName(), durationBehind);
        return CompletableFuture.supplyAsync(() -> {
            boolean shouldStop = false;
            ShardPosition shardPosition = startFrom.shard(getChannelName());
            AtomicInteger pos = new AtomicInteger(shardPosition.startFrom() == StartFrom.HORIZON
                    ? -1
                    : Integer.valueOf(shardPosition.position()));
            do {
                if (hasMessageAfter(pos.get())) {
                    final int index = pos.incrementAndGet();
                    final Message<String> receivedMessage = eventQueue.get(index);
                    LOG.info("Received message from channel={} at position={}: message={}", getChannelName(), index, receivedMessage);
                    final Message<String> interceptedMessage = intercept(receivedMessage);
                    if (interceptedMessage != null) {
                        getMessageDispatcher().accept(interceptedMessage);
                    }
                } else {
                    try {
                        Thread.sleep(100);
                    } catch (final InterruptedException e) {
                        /* ignore */
                    }
                }
                shouldStop = stopCondition.test(shardPosition);
            } while (!shouldStop && !stopSignal.get());
            publishEvent(MessageReceiverStatus.FINISHED, "Finished InMemoryChannel " + getChannelName(), durationBehind);
            return channelPosition(fromPosition(getChannelName(), String.valueOf(pos)));
        }, newSingleThreadExecutor());
    }

    @Override
    public CompletableFuture<Void> consume() {
        publishEvent(MessageReceiverStatus.STARTING, "Starting InMemoryChannel " + getChannelName(), null);
        final Message<String> lastMessage = eventQueue.isEmpty() ? null : Iterables.getLast(eventQueue);
        final ChannelDurationBehind durationBehind = lastMessage != null
                ? ChannelDurationBehind.channelDurationBehind().with(getChannelName(), between(lastMessage.getHeader().getArrivalTimestamp(), now())).build()
                : null;
        publishEvent(MessageReceiverStatus.STARTED, "Started InMemoryChannel " + getChannelName(), durationBehind);
        return CompletableFuture.supplyAsync(() -> {
            do {
                if (!eventQueue.isEmpty()) {
                    final Message<String> receivedMessage = eventQueue.remove(0);
                    final Message<String> interceptedMessage = intercept(
                            message(
                                    receivedMessage.getKey(),
                                    responseHeader(null, now()),
                                    receivedMessage.getPayload()
                            )
                    );
                    if (interceptedMessage != null) {
                        getMessageDispatcher().accept(interceptedMessage);
                    }
                } else {
                    try {
                        Thread.sleep(100);
                    } catch (final InterruptedException e) {
                        /* ignore */
                    }
                }
            } while (!stopSignal.get());
            publishEvent(MessageReceiverStatus.FINISHED, "Finished InMemoryChannel " + getChannelName(), durationBehind);
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
