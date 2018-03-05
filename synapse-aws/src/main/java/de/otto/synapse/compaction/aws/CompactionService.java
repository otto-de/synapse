package de.otto.synapse.compaction.aws;

import de.otto.synapse.aws.s3.SnapshotWriteService;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.consumer.DefaultMessageConsumer;
import de.otto.synapse.eventsource.EventSource;
import de.otto.synapse.eventsource.EventSourceBuilder;
import de.otto.synapse.message.Message;
import de.otto.synapse.state.ConcurrentStateRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.function.Predicate;

import static java.time.Duration.ofSeconds;

public class CompactionService {

    private static final Logger LOG = LoggerFactory.getLogger(CompactionService.class);

    private final ConcurrentStateRepository<String> stateRepository;
    private final SnapshotWriteService snapshotWriteService;
    private final EventSourceBuilder eventSourceBuilder;

    public CompactionService(final SnapshotWriteService snapshotWriteService,
                             final ConcurrentStateRepository<String> concurrentStateRepository,
                             final EventSourceBuilder eventSourceBuilder)
    {
        this.snapshotWriteService = snapshotWriteService;
        this.stateRepository = concurrentStateRepository;
        this.eventSourceBuilder = eventSourceBuilder;
    }

    public String compact(final String streamName) {
        LOG.info("Start compacting stream {}", streamName);
        stateRepository.clear();

        LOG.info("Start loading entries into inMemoryCache from snapshot");

        final EventSource compactingKinesisEventSource = eventSourceBuilder.buildEventSource("compactionSource", streamName);
        compactingKinesisEventSource.register(
                new DefaultMessageConsumer<>(".*", String.class, stateRepository)
        );

        try {
            final ChannelPosition currentPosition = compactingKinesisEventSource.consumeAll(stopCondition());

            LOG.info("Finished updating snapshot data. StateRepository now holds {} entries.", stateRepository.size());

            return snapshotWriteService.writeSnapshot(streamName, currentPosition, stateRepository);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            stateRepository.clear();
        }
    }

    private Predicate<Message<?>> stopCondition() {
        return event -> event.getHeader().getDurationBehind()
                .map(CompactionService::isLessThan10Seconds)
                .orElse(true);
    }

    private static Boolean isLessThan10Seconds(Duration d) {
        return d.compareTo(ofSeconds(10)) < 0;
    }

}

