package de.otto.edison.eventsourcing.compaction;

import de.otto.edison.eventsourcing.EventSourceBuilder;
import de.otto.edison.eventsourcing.consumer.DefaultEventConsumer;
import de.otto.edison.eventsourcing.event.Event;
import de.otto.edison.eventsourcing.consumer.EventSource;
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

import static java.time.Duration.ofSeconds;

@Service
@ConditionalOnProperty(name = "edison.eventsourcing.compaction.enabled", havingValue = "true")
public class CompactionService {

    private static final Logger LOG = LoggerFactory.getLogger(CompactionService.class);

    private final SnapshotWriteService snapshotWriteService;
    private final StateRepository<String> stateRepository;
    private final EventSourceBuilder eventSourceBuilder;

    @Autowired
    public CompactionService(
            SnapshotWriteService snapshotWriteService,
            StateRepository<String> compactionStateRepository,
            EventSourceBuilder defaultEventSourceBuilder)
    {
        this.snapshotWriteService = snapshotWriteService;
        this.stateRepository = compactionStateRepository;
        this.eventSourceBuilder = defaultEventSourceBuilder;
    }

    public String compact(final String streamName) {
        LOG.info("Start compacting stream {}", streamName);
        stateRepository.clear();

        LOG.info("Start loading entries into inMemoryCache from snapshot");

        final EventSource compactingKinesisEventSource = eventSourceBuilder.buildEventSource("compactionSource", streamName);
        compactingKinesisEventSource.register(
                new DefaultEventConsumer<>(".*", String.class, stateRepository)
        );

        try {
            final StreamPosition currentPosition = compactingKinesisEventSource.consumeAll(stopCondition());

            LOG.info("Finished updating snapshot data. StateRepository now holds {} entries.", stateRepository.size());

            return snapshotWriteService.takeSnapshot(streamName, currentPosition, stateRepository);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            stateRepository.clear();
        }
    }

    private Predicate<Event<?>> stopCondition() {
        return event -> event.getDurationBehind()
                .map(CompactionService::isLessThan10Seconds)
                .orElse(true);
    }

    private static Boolean isLessThan10Seconds(Duration d) {
        return d.compareTo(ofSeconds(10)) < 0;
    }

}

