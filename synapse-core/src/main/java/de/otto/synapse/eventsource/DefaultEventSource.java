package de.otto.synapse.eventsource;

import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.ShardResponse;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpoint;
import de.otto.synapse.logging.LogHelper;
import de.otto.synapse.messagestore.MessageStore;
import de.otto.synapse.messagestore.MessageStoreEntry;
import org.slf4j.Logger;
import org.slf4j.MDC;
import org.slf4j.Marker;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.slf4j.LoggerFactory.getLogger;

public class DefaultEventSource extends AbstractEventSource {

    private static final Logger LOG = getLogger(DefaultEventSource.class);
    private static final int LOG_MESSAGE_COUNTER_EVERY_NTH_MESSAGE = 100_000;

    private final MessageStore messageStore;
    private final Marker marker;

    public DefaultEventSource(final @Nonnull MessageStore messageStore,
                              final @Nonnull MessageLogReceiverEndpoint messageLog) {
        super(messageLog);
        this.messageStore = messageStore;
        this.marker = null;
    }

    public DefaultEventSource(final @Nonnull MessageStore messageStore,
                              final @Nonnull MessageLogReceiverEndpoint messageLog,
                              final @Nonnull Marker marker) {
        super(messageLog);
        this.messageStore = messageStore;
        this.marker = marker;
    }

    @Nonnull
    @Override
    public CompletableFuture<ChannelPosition> consumeUntil(final @Nonnull Predicate<ShardResponse> stopCondition) {
        return consumeMessageStore()
                .thenCompose(channelPosition -> getMessageLogReceiverEndpoint().consumeUntil(channelPosition, stopCondition))
                .handle((channelPosition, throwable) -> {
                    if (throwable != null) {
                        LOG.error(marker, "Failed to start consuming from EventSource {}: {}. Closing MessageStore.", getChannelName(), throwable.getMessage(), throwable);
                    }
                    try {
                        messageStore.close();
                    } catch (final Exception e) {
                        LOG.error(marker, "Unable to close() MessageStore: " + e.getMessage(), e);
                    }
                    return channelPosition;
                });
    }


    private CompletableFuture<ChannelPosition> consumeMessageStore() {

        int numberOfDispatcherThreads = 1;
        if (messageStore.isCompacting()) {
            numberOfDispatcherThreads = Runtime.getRuntime().availableProcessors();
        }

        final ExecutorService executorService = Executors.newCachedThreadPool(new CustomizableThreadFactory("synapse-messagestore-dispatcher-"));
        final Semaphore lock = new Semaphore(numberOfDispatcherThreads);
        final String channelName = getChannelName();

        LOG.info(marker, "Starting to read message store for channel '{}'.", channelName);
        Instant startTime = Instant.now();

        final AtomicLong messageCounter = new AtomicLong();
        final AtomicLong firstMessageLogTime = new AtomicLong(System.currentTimeMillis());
        final AtomicLong lastMessageLogTime = new AtomicLong(System.currentTimeMillis());

        final Map<String, String> copyOfContextMap = MDC.getCopyOfContextMap();

        return CompletableFuture.supplyAsync(() -> {
            MDC.setContextMap(copyOfContextMap);
            messageStore
                    .stream()
                    .filter(entry -> entry.getChannelName().equals(channelName))
                    .map(MessageStoreEntry::getTextMessage)
                    .map(message -> getMessageLogReceiverEndpoint().intercept(message))
                    .filter(Objects::nonNull)
                    .forEach(message -> {
                        try {
                            lock.acquire();
                        } catch (InterruptedException e) {
                            LOG.error(marker, e.getMessage(), e);
                        }
                        executorService.execute(() -> {
                            MDC.setContextMap(copyOfContextMap);
                            try {
                                getMessageLogReceiverEndpoint().getMessageDispatcher().accept(message);
                            } finally {
                                long counter = messageCounter.getAndIncrement();
                                if (counter > 0 && counter % LOG_MESSAGE_COUNTER_EVERY_NTH_MESSAGE == 0) {
                                    double messagesPerSecond = LogHelper.calculateMessagesPerSecond(lastMessageLogTime, LOG_MESSAGE_COUNTER_EVERY_NTH_MESSAGE);
                                    LOG.info(marker, "Consumed {} messages ({} per second) from message store for channel '{}'", counter, String.format( "%.2f", messagesPerSecond), channelName );
                                }
                                lock.release();
                            }
                        });
                    });
            executorService.shutdown();
            try {
                executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
                LOG.info(marker, "Consumed a total of {} messages from message store for channel '{}', totalMessagesPerSecond={}", messageCounter.get(), channelName, String.format( "%.2f", LogHelper.calculateMessagesPerSecond(firstMessageLogTime, messageCounter.get())));
            } catch (InterruptedException e) {
                LOG.error(marker, e.getMessage(), e);
            }

            LOG.info(marker, "Finished reading message store for channel '{}'. Duration was {}.", channelName, Duration.between(startTime, Instant.now()));

            return messageStore.getLatestChannelPosition(channelName);
        }, newSingleThreadExecutor(
                new CustomizableThreadFactory("synapse-eventsource-")
        ));
    }

}
