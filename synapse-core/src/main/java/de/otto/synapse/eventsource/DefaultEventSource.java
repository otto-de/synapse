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
import java.util.concurrent.CompletableFuture;
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
        final String channelName = getChannelName();
        return CompletableFuture.supplyAsync(() -> {
            messageStore
                    .stream()
                    .filter(entry -> entry.getChannelName().equals(channelName))
                    .map(MessageStoreEntry::getTextMessage)
                    .map(message -> getMessageLogReceiverEndpoint().intercept(message))
                    .filter(Objects::nonNull)
                    .forEach(message -> {
                        getMessageLogReceiverEndpoint().getMessageDispatcher().accept(message);
                    });
            return messageStore.getLatestChannelPosition(channelName);
        }, newSingleThreadExecutor(
                new CustomizableThreadFactory("synapse-eventsource-")
        ));
    }

}
