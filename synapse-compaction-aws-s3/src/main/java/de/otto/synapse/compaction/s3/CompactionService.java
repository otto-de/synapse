package de.otto.synapse.compaction.s3;

import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpoint;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpointFactory;
import de.otto.synapse.eventsource.DefaultEventSource;
import de.otto.synapse.eventsource.EventSource;
import de.otto.synapse.eventsource.EventSourceBuilder;
import de.otto.synapse.messagestore.MessageStore;
import de.otto.synapse.messagestore.MessageStoreFactory;
import de.otto.synapse.state.StateRepository;
import de.otto.synapse.translator.MessageFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

import java.time.Clock;

import static de.otto.synapse.channel.StopCondition.*;
import static de.otto.synapse.translator.MessageFormat.defaultMessageFormat;

public class CompactionService {

    private static final Logger LOG = LoggerFactory.getLogger(CompactionService.class);

    private final StateRepository<String> stateRepository;
    private final SnapshotWriteService snapshotWriteService;
    private final MessageLogReceiverEndpointFactory messageLogReceiverEndpointFactory;
    private final MessageStoreFactory<? extends MessageStore> snapshotMessageStoreFactory;
    private final Clock clock;

    public CompactionService(final SnapshotWriteService snapshotWriteService,
                             final StateRepository<String> stateRepository,
                             final MessageLogReceiverEndpointFactory messageLogReceiverEndpointFactory,
                             final MessageStoreFactory<? extends MessageStore> messageStoreFactory)
    {
        this(snapshotWriteService, stateRepository, messageLogReceiverEndpointFactory, messageStoreFactory, Clock.systemDefaultZone());
    }

    public CompactionService(final SnapshotWriteService snapshotWriteService,
                             final StateRepository<String> stateRepository,
                             final MessageLogReceiverEndpointFactory messageLogReceiverEndpointFactory,
                             final MessageStoreFactory<? extends MessageStore> messageStoreFactory,
                             final Clock clock)
    {
        this.snapshotWriteService = snapshotWriteService;
        this.stateRepository = stateRepository;
        this.messageLogReceiverEndpointFactory = messageLogReceiverEndpointFactory;
        this.snapshotMessageStoreFactory = messageStoreFactory;
        this.clock = clock;
    }

    public String compact(final String channelName, final MessageFormat messageFormat, final Marker marker) {
        LOG.info(marker, "Start compacting channel {} with MessageFormat {}", channelName, messageFormat);
        stateRepository.clear();

        LOG.info(marker, "Start loading entries from snapshot");
        final MessageLogReceiverEndpoint messageLog = messageLogReceiverEndpointFactory.create(channelName);
        final MessageStore messageStore = snapshotMessageStoreFactory.createMessageStoreFor(channelName);
        final EventSource compactingKinesisEventSource = new DefaultEventSource(messageStore, messageLog, marker);

        compactingKinesisEventSource.register(
                new SnapshotMessageConsumer(
                        messageFormat,
                        stateRepository)
        );
        LOG.info(marker, "Reading event source until either end of channel is reached or messages are younger than {}.", clock.instant());
        try {
            final ChannelPosition currentPosition = compactingKinesisEventSource
                    .consumeUntil(
                            endOfChannel()
                                    .and(emptyResponse())
                                    .or(arrivalTimestampAfterNow(clock))
                    )
                    .get();

            LOG.info(marker, "Finished updating snapshot data. StateRepository now holds {} entries.", stateRepository.size());
            return snapshotWriteService.writeSnapshot(channelName, currentPosition, stateRepository);
        } catch (Exception e) {
            LOG.error(marker, "Exception during compaction.", e);
            throw new RuntimeException(e);
        } finally {
            stateRepository.clear();
        }
    }

    public String compact(final String channelName) {
        return compact(channelName, defaultMessageFormat(), null);
    }

    public String compact(final String channelName, final Marker marker) {
        return compact(channelName, defaultMessageFormat(), marker);
    }

}

