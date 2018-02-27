package de.otto.synapse.aws;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.EventSourceBuilder;
import de.otto.synapse.aws.s3.SnapshotConsumerService;
import de.otto.synapse.aws.s3.SnapshotEventSource;
import de.otto.synapse.aws.s3.SnapshotReadService;
import de.otto.synapse.consumer.EventSource;
import org.slf4j.Logger;
import org.springframework.context.ApplicationEventPublisher;

import java.util.Objects;

import static org.slf4j.LoggerFactory.getLogger;

public class SnapshotEventSourceBuilder implements EventSourceBuilder {

    private static final Logger LOG = getLogger(SnapshotEventSourceBuilder.class);

    private final SnapshotReadService snapshotReadService;
    private final SnapshotConsumerService snapshotConsumerService;

    private final ObjectMapper objectMapper;

    private final ApplicationEventPublisher applicationEventPublisher;

    public SnapshotEventSourceBuilder(final SnapshotReadService snapshotReadService,
                                      final SnapshotConsumerService snapshotConsumerService,
                                      final ObjectMapper objectMapper,
                                      final ApplicationEventPublisher applicationEventPublisher) {
        this.snapshotReadService = snapshotReadService;
        this.snapshotConsumerService = snapshotConsumerService;
        this.objectMapper = objectMapper;
        this.applicationEventPublisher = applicationEventPublisher;
    }

    @Override
    public EventSource buildEventSource(final String name, final String streamName) {
        Objects.requireNonNull(streamName, "stream name must not be null");
        LOG.info("Building '{}' as SnapshotEventSource", streamName);
        return new SnapshotEventSource(
                name,
                streamName,
                snapshotReadService,
                snapshotConsumerService,
                applicationEventPublisher,
                objectMapper
        );
    }

}
