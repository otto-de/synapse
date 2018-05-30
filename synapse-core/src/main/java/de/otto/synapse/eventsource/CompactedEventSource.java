package de.otto.synapse.eventsource;

import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpoint;
import de.otto.synapse.message.Message;
import de.otto.synapse.messagestore.MessageStore;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.time.Instant;

import static org.slf4j.LoggerFactory.getLogger;

public class CompactedEventSource extends AbstractEventSource {

    private static final Logger LOG = getLogger(CompactedEventSource.class);

    private final MessageStore messageStore;

    public CompactedEventSource(final @Nonnull String name,
                                final @Nonnull MessageStore messageStore,
                                final @Nonnull MessageLogReceiverEndpoint messageLog) {
        super(name, messageLog);
        this.messageStore = messageStore;
    }

    @Nonnull
    @Override
    public ChannelPosition consumeUntil(final @Nonnull ChannelPosition startFrom,
                                        final @Nonnull Instant until) {

        try {
            messageStore.stream().forEach(message -> {
                final Message<String> interceptedMessage = getMessageLogReceiverEndpoint().getInterceptorChain().intercept(message);
                if (interceptedMessage != null) {
                    getMessageDispatcher().accept(interceptedMessage);
                }
            });

            return getMessageLogReceiverEndpoint().consumeUntil(messageStore.getLatestChannelPosition(), until);
        } finally {
            try {
                messageStore.close();
            } catch (final Exception e) {
                LOG.error("Unable to close() MessageStore: " + e.getMessage(), e);
            }
        }
    }

}
