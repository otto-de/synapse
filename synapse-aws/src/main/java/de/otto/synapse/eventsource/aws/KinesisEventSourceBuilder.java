package de.otto.synapse.eventsource.aws;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.channel.aws.KinesisMessageLogReceiverEndpoint;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpoint;
import de.otto.synapse.eventsource.EventSource;
import de.otto.synapse.eventsource.EventSourceBuilder;
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
        final MessageLogReceiverEndpoint messageLog = new KinesisMessageLogReceiverEndpoint(kinesisClient, objectMapper, streamName);
        return new KinesisEventSource(name, messageLog, eventPublisher);
    }

}
