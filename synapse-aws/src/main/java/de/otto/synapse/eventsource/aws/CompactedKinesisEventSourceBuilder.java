package de.otto.synapse.eventsource.aws;

import de.otto.synapse.eventsource.EventSource;
import de.otto.synapse.eventsource.EventSourceBuilder;
import org.slf4j.Logger;

import java.util.Objects;

import static org.slf4j.LoggerFactory.getLogger;

public class CompactedKinesisEventSourceBuilder implements EventSourceBuilder {

    private static final Logger LOG = getLogger(CompactedKinesisEventSourceBuilder.class);

    private final KinesisEventSourceBuilder kinesisEventSourceBuilder;
    private final SnapshotEventSourceBuilder snapshotEventSourceBuilder;

    public CompactedKinesisEventSourceBuilder(final KinesisEventSourceBuilder kinesisEventSourceBuilder,
                                              final SnapshotEventSourceBuilder snapshotEventSourceBuilder) {
        this.kinesisEventSourceBuilder = kinesisEventSourceBuilder;
        this.snapshotEventSourceBuilder = snapshotEventSourceBuilder;
    }

    public EventSource buildEventSource(final String name, final String streamName) {
        Objects.requireNonNull(streamName, "stream name must not be null");
        LOG.info("Building '{}' as CompactingKinesisEventSource", streamName);
        return new CompactedKinesisEventSource(
                snapshotEventSourceBuilder.buildEventSource(name, streamName),
                kinesisEventSourceBuilder.buildEventSource(name, streamName)
        );
    }

}
