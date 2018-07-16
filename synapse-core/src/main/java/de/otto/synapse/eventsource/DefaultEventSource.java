package de.otto.synapse.eventsource;

import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpoint;
import de.otto.synapse.message.Message;
import de.otto.synapse.messagestore.MessageStore;
import org.slf4j.Logger;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadFactory;

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
    public CompletableFuture<ChannelPosition> consumeUntil(final @Nonnull Instant until) {
        return consumeMessageStore()
                .thenCompose(messageLogStartPosition -> getMessageLogReceiverEndpoint().consumeUntil(messageLogStartPosition, until))
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
        final ThreadFactory threadFactory = new CustomizableThreadFactory("kinesis-eventsource-");
        return CompletableFuture.supplyAsync(() -> {
            messageStore.stream().forEach(message -> {
                final Message<String> interceptedMessage = getMessageLogReceiverEndpoint().getInterceptorChain().intercept(message);
                if (interceptedMessage != null) {
                    getMessageLogReceiverEndpoint().getMessageDispatcher().accept(interceptedMessage);
                }
            });
            return messageStore.getLatestChannelPosition();
        }, newSingleThreadExecutor(threadFactory));
    }

}
