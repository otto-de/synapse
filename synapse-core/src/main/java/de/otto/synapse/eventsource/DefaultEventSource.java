package de.otto.synapse.eventsource;

import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpoint;
import de.otto.synapse.message.Message;
import de.otto.synapse.messagestore.MessageStore;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.time.Instant;

import static de.otto.synapse.channel.ChannelPosition.fromHorizon;
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
    public ChannelPosition consumeUntil(final @Nonnull ChannelPosition startFrom,
                                        final @Nonnull Instant until) {

        try {
            final ChannelPosition messageLogStartPosition;
            if (startFrom.equals(fromHorizon())) {
                messageStore.stream().forEach(message -> {
                    final Message<String> interceptedMessage = getMessageLogReceiverEndpoint().getInterceptorChain().intercept(message);
                    if (interceptedMessage != null) {
                        getMessageLogReceiverEndpoint().getMessageDispatcher().accept(interceptedMessage);
                    }
                });
                messageLogStartPosition = messageStore.getLatestChannelPosition();
            } else {
                messageLogStartPosition = startFrom;
            }
            return getMessageLogReceiverEndpoint().consumeUntil(messageLogStartPosition, until);
        } finally {
            try {
                messageStore.close();
            } catch (final Exception e) {
                LOG.error("Unable to close() MessageStore: " + e.getMessage(), e);
            }
        }
    }

}
