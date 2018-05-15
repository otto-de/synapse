package de.otto.synapse.eventsource.aws;

import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpoint;
import de.otto.synapse.eventsource.AbstractEventSource;
import de.otto.synapse.eventsource.EventSourceNotification;
import de.otto.synapse.message.Message;
import org.springframework.context.ApplicationEventPublisher;

import javax.annotation.Nonnull;
import java.util.function.Predicate;

import static de.otto.synapse.eventsource.EventSourceNotification.Status.FINISHED;

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
    public ChannelPosition consume(final ChannelPosition startFrom,
                                   final Predicate<Message<?>> stopCondition) {
        publishEvent(startFrom, EventSourceNotification.Status.STARTED, "Consuming messages from Kinesis.");
        try {
            ChannelPosition currentPosition = messageLog.consume(startFrom, stopCondition);
            publishEvent(currentPosition, FINISHED, "Stopped consuming messages from Kinesis.");
            return currentPosition;
        } catch (final RuntimeException e) {
            publishEvent(startFrom, EventSourceNotification.Status.FAILED, "Error consuming messages from Kinesis: " + e.getMessage());
            throw e;
        }

    }

    MessageLogReceiverEndpoint getMessageLogReceiverEndpoint() {
        return messageLog;
    }
}
