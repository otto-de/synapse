package de.otto.synapse.eventsource.aws;

import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpoint;
import de.otto.synapse.eventsource.AbstractEventSource;
import de.otto.synapse.info.MessageEndpointStatus;
import de.otto.synapse.message.Message;
import org.springframework.context.ApplicationEventPublisher;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.util.function.Predicate;

import static de.otto.synapse.info.MessageEndpointStatus.FINISHED;

public class KinesisEventSource extends AbstractEventSource {

    private final MessageLogReceiverEndpoint messageLog;

    public KinesisEventSource(final String name,
                              final MessageLogReceiverEndpoint messageLog,
                              final ApplicationEventPublisher eventPublisher) {
        super(name, messageLog, eventPublisher);
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
        publishEvent(startFrom, MessageEndpointStatus.STARTING, "Consuming messages from Kinesis.");
        try {
            ChannelPosition currentPosition = messageLog.consumeUntil(startFrom, until);
            publishEvent(currentPosition, FINISHED, "Stopped consuming messages from Kinesis.");
            return currentPosition;
        } catch (final RuntimeException e) {
            publishEvent(startFrom, MessageEndpointStatus.FAILED, "Error consuming messages from Kinesis: " + e.getMessage());
            throw e;
        }

    }

    MessageLogReceiverEndpoint getMessageLogReceiverEndpoint() {
        return messageLog;
    }
}
