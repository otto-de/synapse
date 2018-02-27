package de.otto.edison.eventsourcing.aws;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.eventsourcing.EventSourceBuilder;
import de.otto.edison.eventsourcing.aws.kinesis.KinesisEventSource;
import de.otto.edison.eventsourcing.aws.kinesis.KinesisMessageLog;
import de.otto.edison.eventsourcing.aws.kinesis.MessageLog;
import de.otto.edison.eventsourcing.consumer.EventSource;
import org.slf4j.Logger;
import org.springframework.context.ApplicationEventPublisher;
import software.amazon.awssdk.services.kinesis.KinesisClient;

import java.util.Objects;

import static org.slf4j.LoggerFactory.getLogger;

public class KinesisEventSourceBuilder implements EventSourceBuilder {

    private static final Logger LOG = getLogger(KinesisEventSourceBuilder.class);

    private final ObjectMapper objectMapper;
    private final ApplicationEventPublisher eventPublisher;
    private final KinesisClient kinesisClient;

    public KinesisEventSourceBuilder(final ObjectMapper objectMapper,
                                     final ApplicationEventPublisher eventPublisher,
                                     final KinesisClient kinesisClient) {
        this.objectMapper = objectMapper;
        this.eventPublisher = eventPublisher;
        this.kinesisClient = kinesisClient;
    }

    @Override
    public EventSource buildEventSource(final String name, final String streamName) {
        Objects.requireNonNull(streamName, "stream name must not be null");
        LOG.info("Building '{}' as KinesisEventSource", streamName);
        final MessageLog messageLog = new KinesisMessageLog(kinesisClient, streamName);
        return new KinesisEventSource(name, messageLog, eventPublisher, objectMapper);
    }

}
