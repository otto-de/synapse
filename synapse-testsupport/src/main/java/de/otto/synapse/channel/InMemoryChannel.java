package de.otto.synapse.channel;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
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

import static de.otto.synapse.message.Header.responseHeader;
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
        super(channelName, interceptorRegistry, new ObjectMapper().registerModule(new JavaTimeModule()), null);
        this.eventQueue = synchronizedList(new ArrayList<>());
    }

    public InMemoryChannel(final String channelName,
                           final MessageInterceptorRegistry interceptorRegistry,
                           final ObjectMapper objectMapper,
                           final ApplicationEventPublisher eventPublisher) {
        super(channelName, interceptorRegistry, objectMapper, eventPublisher);
        this.eventQueue = synchronizedList(new ArrayList<>());
    }

    public synchronized void send(final Message<String> message) {
        LOG.info("Sending {} to {} at position{}", message, getChannelName(), eventQueue.size());
        eventQueue.add(message);
    }

    @Nonnull
    @Override
    public CompletableFuture<ChannelPosition> consumeUntil(@Nonnull final ChannelPosition startFrom,
                                                           @Nonnull final Instant until) {
        publishEvent(MessageReceiverStatus.STARTING, "Starting InMemoryChannel " + getChannelName(), null);
        final Message<String> lastMessage = eventQueue.isEmpty() ? null : Iterables.getLast(eventQueue);
        final ChannelDurationBehind durationBehind = lastMessage != null
                ? ChannelDurationBehind.channelDurationBehind().with(getChannelName(), between(lastMessage.getHeader().getArrivalTimestamp(), now())).build()
                : null;
        publishEvent(MessageReceiverStatus.STARTED, "Started InMemoryChannel " + getChannelName(), durationBehind);
        return CompletableFuture.supplyAsync(() -> {
            boolean shouldStop = false;
            AtomicInteger pos = new AtomicInteger(startFrom.shard(getChannelName()).startFrom() == StartFrom.HORIZON
                    ? -1
                    : Integer.valueOf(startFrom.shard(getChannelName()).position()));
            do {
                if (hasMessageAfter(pos.get())) {
                    final int index = pos.incrementAndGet();
                    final Message<String> receivedMessage = eventQueue.get(index);
                    LOG.info("Received message from channel={} at position={}: message={}", getChannelName(), index, receivedMessage);
                    final Message<String> interceptedMessage = intercept(
                            Message.message(
                                    receivedMessage.getKey(),
                                    responseHeader(null, now()),
                                    receivedMessage.getPayload()
                            )
                    );
                    if (interceptedMessage != null) {
                        getMessageDispatcher().accept(interceptedMessage);
                        shouldStop = !until.isAfter(interceptedMessage.getHeader().getArrivalTimestamp());
                    }
                } else {
                    shouldStop = !until.isAfter(now());
                    try {
                        Thread.sleep(100);
                    } catch (final InterruptedException e) {
                        /* ignore */
                    }
                }
            } while (!shouldStop && !stopSignal.get());
            publishEvent(MessageReceiverStatus.FINISHED, "Finished InMemoryChannel " + getChannelName(), durationBehind);
            return ChannelPosition.channelPosition(ShardPosition.fromPosition(getChannelName(), String.valueOf(pos)));
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
                            Message.message(
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
