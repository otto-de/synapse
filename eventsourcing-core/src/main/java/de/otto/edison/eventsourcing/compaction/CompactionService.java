package de.otto.edison.eventsourcing.compaction;

import de.otto.edison.eventsourcing.CompactingKinesisEventSource;
import de.otto.edison.eventsourcing.consumer.DefaultEventConsumer;
import de.otto.edison.eventsourcing.consumer.Event;
import de.otto.edison.eventsourcing.consumer.EventConsumer;
import de.otto.edison.eventsourcing.consumer.StreamPosition;
import de.otto.edison.eventsourcing.s3.SnapshotConsumerService;
import de.otto.edison.eventsourcing.s3.SnapshotReadService;
import de.otto.edison.eventsourcing.s3.SnapshotWriteService;
import de.otto.edison.eventsourcing.state.StateRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.kinesis.KinesisClient;

import java.time.Instant;
import java.util.function.Function;
import java.util.function.Predicate;

@Service
@ConditionalOnProperty(name = "edison.eventsourcing.compaction.enabled", havingValue = "true")
public class CompactionService {

    private static final Logger LOG = LoggerFactory.getLogger(CompactionService.class);

    private final SnapshotReadService snapshotReadService;
    private final SnapshotWriteService snapshotWriteService;
    private final SnapshotConsumerService snapshotConsumerService;
    private final KinesisClient kinesisClient;
    private final StateRepository<String> stateRepository;

    @Autowired
    public CompactionService(
            SnapshotReadService snapshotReadService,
            SnapshotWriteService snapshotWriteService,
            SnapshotConsumerService snapshotConsumerService,
            KinesisClient kinesisClient,
            StateRepository<String> stateRepository)
    {
        this.snapshotReadService = snapshotReadService;
        this.snapshotWriteService = snapshotWriteService;
        this.snapshotConsumerService = snapshotConsumerService;
        this.kinesisClient = kinesisClient;
        this.stateRepository = stateRepository;
    }

    public String compact(final String streamName) {
        LOG.info("Start compacting stream {}", streamName);
        LOG.info(stateRepository.getStats());
        stateRepository.clear();

        LOG.info("Start loading entries into inMemoryCache from snapshot");
        CompactingKinesisEventSource<String> compactingKinesisEventSource = new CompactingKinesisEventSource<>(streamName,
                String.class, snapshotReadService, snapshotConsumerService, Function.identity(), kinesisClient);
        try {
            EventConsumer<String> consumer = new DefaultEventConsumer<>(streamName, ".*", stateRepository);
            StreamPosition currentPosition = compactingKinesisEventSource.consumeAll(stopCondition(), consumer.consumerFunction());

            LOG.info("Finished updating snapshot data. StateRepository now holds {} entries.", stateRepository.size());

            return snapshotWriteService.takeSnapshot(streamName, currentPosition, stateRepository);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Predicate<Event<String>> stopCondition() {
        final Instant now = Instant.now();
        return event -> {
            if (event != null) {
                return event.arrivalTimestamp().isAfter(now);
            } else {
                return true;
            }
        };
    }

}

