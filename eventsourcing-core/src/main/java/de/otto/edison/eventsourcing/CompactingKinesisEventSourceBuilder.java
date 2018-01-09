package de.otto.edison.eventsourcing;

import de.otto.edison.eventsourcing.consumer.EventSource;
import org.slf4j.Logger;
import org.springframework.stereotype.Component;

import java.util.Objects;

import static org.slf4j.LoggerFactory.getLogger;

@Component
public class CompactingKinesisEventSourceBuilder implements EventSourceBuilder {

    private static final Logger LOG = getLogger(CompactingKinesisEventSourceBuilder.class);

    private final KinesisEventSourceBuilder kinesisEventSourceBuilder;
    private final SnapshotEventSourceBuilder snapshotEventSourceBuilder;

    public CompactingKinesisEventSourceBuilder(final KinesisEventSourceBuilder kinesisEventSourceBuilder,
                                               final SnapshotEventSourceBuilder snapshotEventSourceBuilder) {
        this.kinesisEventSourceBuilder = kinesisEventSourceBuilder;
        this.snapshotEventSourceBuilder = snapshotEventSourceBuilder;
    }

    public EventSource buildEventSource(final String streamName) {
        Objects.requireNonNull(streamName, "stream name must not be null");
        LOG.info("Building '{}' as CompactingKinesisEventSource", streamName);
        return new CompactingKinesisEventSource(
                snapshotEventSourceBuilder.buildEventSource(streamName),
                kinesisEventSourceBuilder.buildEventSource(streamName)
        );
    }

}
