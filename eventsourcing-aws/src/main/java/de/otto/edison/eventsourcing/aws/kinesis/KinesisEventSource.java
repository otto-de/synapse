package de.otto.edison.eventsourcing.aws.kinesis;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.eventsourcing.consumer.AbstractEventSource;
import de.otto.edison.eventsourcing.consumer.EventSourceNotification;
import de.otto.edison.eventsourcing.consumer.StreamPosition;
import de.otto.edison.eventsourcing.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;

import java.util.function.Predicate;

import static de.otto.edison.eventsourcing.consumer.EventSourceNotification.Status.FINISHED;
import static java.lang.Thread.sleep;

public class KinesisEventSource extends AbstractEventSource {

    private static final Logger LOG = LoggerFactory.getLogger(KinesisEventSource.class);

    private final KinesisStream kinesisStream;

    public KinesisEventSource(final String name,
                              final KinesisStream kinesisStream,
                              final ApplicationEventPublisher eventPublisher,
                              final ObjectMapper objectMapper) {
        super(name, eventPublisher, objectMapper);
        this.kinesisStream = kinesisStream;
    }

    @Override
    public String getStreamName() {
        return kinesisStream.getStreamName();
    }

    @Override
    public void stop() {
        super.stop();
    }

    @Override
    public StreamPosition consumeAll(final StreamPosition startFrom,
                                     final Predicate<Message<?>> stopCondition) {
        StreamPosition currentPosition = startFrom;
        publishEvent(startFrom, EventSourceNotification.Status.STARTED, "Consuming messages from Kinesis.");
        try {
            StreamResponse streamResponse;
            boolean consumeMore;
            do {
                streamResponse = kinesisStream.consumeStream(currentPosition, stopCondition, registeredConsumers());
                currentPosition = streamResponse.getStreamPosition();
                consumeMore = streamResponse.getStatus() != Status.STOPPED && !isStopping();
                if (consumeMore) {
                    consumeMore = waitABit();
                }
            } while (consumeMore);
            publishEvent(currentPosition, FINISHED, "Stopped consuming messages from Kinesis.");
            return currentPosition;
        } catch (final RuntimeException e) {
            publishEvent(currentPosition, EventSourceNotification.Status.FAILED, "Error consuming messages from Kinesis: " + e.getMessage());
            throw e;
        }

    }

    /**
     * Waits one second before the next page of records is requested from Kinesis.
     *
     * @return false, if waiting was interrupted, true otherwise.
     */
    private boolean waitABit() {
        try {
            /* See DECISIONS.md - Question #1 */
            sleep(1000);
        } catch (InterruptedException e) {
            LOG.warn("Thread got interrupted");
            return false;
        }
        return true;
    }

}
