package de.otto.synapse.eventsource.aws;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.aws.MessageLog;
import de.otto.synapse.consumer.EventSourceNotification;
import de.otto.synapse.eventsource.AbstractEventSource;
import de.otto.synapse.message.Message;
import org.springframework.context.ApplicationEventPublisher;

import java.util.function.Predicate;

import static de.otto.synapse.consumer.EventSourceNotification.Status.FINISHED;

public class KinesisEventSource extends AbstractEventSource {

    private final MessageLog messageLog;

    public KinesisEventSource(final String name,
                              final MessageLog messageLog,
                              final ApplicationEventPublisher eventPublisher,
                              final ObjectMapper objectMapper) {
        super(name, eventPublisher, objectMapper);
        this.messageLog = messageLog;
    }

    @Override
    public String getStreamName() {
        return messageLog.getStreamName();
    }

    @Override
    public void stop() {
        super.stop();
    }

    @Override
    public ChannelPosition consumeAll(final ChannelPosition startFrom,
                                      final Predicate<Message<?>> stopCondition) {
        publishEvent(startFrom, EventSourceNotification.Status.STARTED, "Consuming messages from Kinesis.");
        try {
            ChannelPosition currentPosition = messageLog.consumeStream(startFrom, stopCondition, dispatchingMessageConsumer());
            publishEvent(currentPosition, FINISHED, "Stopped consuming messages from Kinesis.");
            return currentPosition;
        } catch (final RuntimeException e) {
            publishEvent(startFrom, EventSourceNotification.Status.FAILED, "Error consuming messages from Kinesis: " + e.getMessage());
            throw e;
        }

    }

}
