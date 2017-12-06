package de.otto.edison.eventsourcing.compaction;

import de.otto.edison.eventsourcing.CompactingKinesisEventSource;
import de.otto.edison.eventsourcing.EventSourceFactory;
import de.otto.edison.eventsourcing.consumer.DefaultEventConsumer;
import de.otto.edison.eventsourcing.consumer.Event;
import de.otto.edison.eventsourcing.consumer.EventConsumer;
import de.otto.edison.eventsourcing.consumer.StreamPosition;
import de.otto.edison.eventsourcing.s3.SnapshotWriteService;
import de.otto.edison.eventsourcing.state.StateRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.function.Predicate;

@Service
@ConditionalOnProperty(name = "edison.eventsourcing.compaction.enabled", havingValue = "true")
public class CompactionService {

    private static final Logger LOG = LoggerFactory.getLogger(CompactionService.class);

    private final SnapshotWriteService snapshotWriteService;
    private final StateRepository<String> stateRepository;
    private final EventSourceFactory eventSourceFactory;

    @Autowired
    public CompactionService(
            SnapshotWriteService snapshotWriteService,
            StateRepository<String> stateRepository,
            EventSourceFactory eventSourceFactory)
    {
        this.snapshotWriteService = snapshotWriteService;
        this.stateRepository = stateRepository;
        this.eventSourceFactory = eventSourceFactory;
    }

    public String compact(final String streamName) {
        LOG.info("Start compacting stream {}", streamName);
        stateRepository.clear();

        LOG.info("Start loading entries into inMemoryCache from snapshot");

        CompactingKinesisEventSource<String> compactingKinesisEventSource = eventSourceFactory.createCompactingKinesisEventSource(streamName, String.class);

        try {
            EventConsumer<String> consumer = new DefaultEventConsumer<>(streamName, stateRepository);
            StreamPosition currentPosition = compactingKinesisEventSource.consumeAll(stopCondition(), consumer.consumerFunction());

            LOG.info("Finished updating snapshot data. StateRepository now holds {} entries.", stateRepository.size());

            return snapshotWriteService.takeSnapshot(streamName, currentPosition, stateRepository);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Predicate<Event<String>> stopCondition() {
        return event -> event.durationBehind()
                .map(CompactionService::isLessThan10Seconds)
                .orElse(true);
    }

    private static Boolean isLessThan10Seconds(Duration d) {
        return d.compareTo(Duration.ofSeconds(10)) < 0;
    }

}

