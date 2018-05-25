package de.otto.synapse.eventsource.aws;

import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpoint;
import de.otto.synapse.eventsource.AbstractEventSource;

import javax.annotation.Nonnull;
import java.time.Instant;

public class KinesisEventSource extends AbstractEventSource {

    private final MessageLogReceiverEndpoint messageLog;

    public KinesisEventSource(final String name,
                              final MessageLogReceiverEndpoint messageLog) {
        super(name, messageLog);
        this.messageLog = messageLog;
    }

    @Override
    public void stop() {
        super.stop();
        messageLog.stop();
    }

    @Nonnull
    @Override
    public ChannelPosition consumeUntil(final @Nonnull ChannelPosition startFrom,
                                        final @Nonnull Instant until) {
        return messageLog.consumeUntil(startFrom, until);
    }

    MessageLogReceiverEndpoint getMessageLogReceiverEndpoint() {
        return messageLog;
    }
}
