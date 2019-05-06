package de.otto.synapse.eventsource;

import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.ShardResponse;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpoint;
import de.otto.synapse.messagestore.MessageStore;
import de.otto.synapse.messagestore.MessageStoreEntry;
import org.slf4j.Logger;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.slf4j.LoggerFactory.getLogger;

public class DefaultEventSource extends AbstractEventSource {

    private static final Logger LOG = getLogger(DefaultEventSource.class);

    private final MessageStore messageStore;

    public DefaultEventSource(final @Nonnull MessageStore messageStore,
                              final @Nonnull MessageLogReceiverEndpoint messageLog) {
        super(messageLog);
        this.messageStore = messageStore;
    }

    @Nonnull
    @Override
    public CompletableFuture<ChannelPosition> consumeUntil(final @Nonnull Predicate<ShardResponse> stopCondition) {
        return consumeMessageStore()
                .thenCompose(channelPosition -> getMessageLogReceiverEndpoint().consumeUntil(channelPosition, stopCondition))
                .handle((channelPosition, throwable) -> {
                    if (throwable != null) {
                        LOG.error("Failed to start consuming from EventSource {}: {}. Closing MessageStore.", getChannelName(), throwable.getMessage(), throwable);
                    }
                    try {
                        messageStore.close();
                    } catch (final Exception e) {
                        LOG.error("Unable to close() MessageStore: " + e.getMessage(), e);
                    }
                    return channelPosition;
                });
    }


    private CompletableFuture<ChannelPosition> consumeMessageStore() {

        int numberOfDispatcherThreads = 1;
        if (messageStore.isCompacting()) {
            numberOfDispatcherThreads = 16;
        }
        final ExecutorService executorService = Executors.newCachedThreadPool(new CustomizableThreadFactory("synapse-messagestore-dispatcher-"));
        final Semaphore lock = new Semaphore(numberOfDispatcherThreads);

        final String channelName = getChannelName();

        final AtomicBoolean started = new AtomicBoolean(false);

        return CompletableFuture.supplyAsync(() -> {
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
                            LOG.error(e.getMessage(), e);
                        }
                        started.set(true);
                        executorService.execute(() -> {
                            try {
                                getMessageLogReceiverEndpoint().getMessageDispatcher().accept(message);
                            } finally {
                                lock.release();
                            }
                        });
                    });
            executorService.shutdown();
            try {
                executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                LOG.error(e.getMessage(), e);
            }
            return messageStore.getLatestChannelPosition(channelName);
        }, newSingleThreadExecutor(
                new CustomizableThreadFactory("synapse-eventsource-")
        ));
    }

}
