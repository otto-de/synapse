package de.otto.synapse.eventsource.aws;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.compaction.aws.SnapshotConsumerService;
import de.otto.synapse.compaction.aws.SnapshotEventSource;
import de.otto.synapse.compaction.aws.SnapshotReadService;
import de.otto.synapse.eventsource.EventSource;
import de.otto.synapse.eventsource.EventSourceBuilder;
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
    public EventSource buildEventSource(final String name, final String channelName) {
        Objects.requireNonNull(channelName, "stream name must not be null");
        LOG.info("Building '{}' as SnapshotEventSource", channelName);
        return new SnapshotEventSource(
                name,
                channelName,
                snapshotReadService,
                snapshotConsumerService,
                applicationEventPublisher,
                objectMapper
        );
    }

}
