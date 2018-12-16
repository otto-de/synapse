package de.otto.synapse.compaction.s3;

import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.StopCondition;
import de.otto.synapse.consumer.StatefulMessageConsumer;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpoint;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpointFactory;
import de.otto.synapse.eventsource.EventSource;
import de.otto.synapse.eventsource.EventSourceBuilder;
import de.otto.synapse.message.Message;
import de.otto.synapse.state.StateRepository;
import de.otto.synapse.translator.MessageCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;

import static de.otto.synapse.channel.StopCondition.*;
import static java.time.Instant.now;

public class CompactionService {

    private static final Logger LOG = LoggerFactory.getLogger(CompactionService.class);

    private final StateRepository<String> stateRepository;
    private final SnapshotWriteService snapshotWriteService;
    private final EventSourceBuilder eventSourceBuilder;
    private final MessageLogReceiverEndpointFactory messageLogReceiverEndpointFactory;
    private final Clock clock;

    public CompactionService(final SnapshotWriteService snapshotWriteService,
                             final StateRepository<String> stateRepository,
                             final EventSourceBuilder eventSourceBuilder,
                             final MessageLogReceiverEndpointFactory messageLogReceiverEndpointFactory)
    {
        this(snapshotWriteService, stateRepository, eventSourceBuilder, messageLogReceiverEndpointFactory, Clock.systemDefaultZone());
    }

    public CompactionService(final SnapshotWriteService snapshotWriteService,
                             final StateRepository<String> stateRepository,
                             final EventSourceBuilder eventSourceBuilder,
                             final MessageLogReceiverEndpointFactory messageLogReceiverEndpointFactory,
                             final Clock clock)
    {
        this.snapshotWriteService = snapshotWriteService;
        this.stateRepository = stateRepository;
        this.eventSourceBuilder = eventSourceBuilder;
        this.messageLogReceiverEndpointFactory = messageLogReceiverEndpointFactory;
        this.clock = clock;
    }

    public String compact(final String channelName) {
        LOG.info("Start compacting channel {}", channelName);
        stateRepository.clear();

        LOG.info("Start loading entries into inMemoryCache from snapshot");
        final MessageLogReceiverEndpoint messageLog = messageLogReceiverEndpointFactory.create(channelName);
        final EventSource compactingKinesisEventSource = eventSourceBuilder.buildEventSource(messageLog);
        compactingKinesisEventSource.register(
                new StatefulMessageConsumer<>(".*", String.class, stateRepository, MessageCodec::encode, Message::getKey)
        );

        try {
            final ChannelPosition currentPosition = compactingKinesisEventSource
                    .consumeUntil(
                            endOfChannel()
                                    .and(emptyResponse())
                                    .or(arrivalTimeAfterNow(clock))
                    )
                    .get();

            LOG.info("Finished updating snapshot data. StateRepository now holds {} entries.", stateRepository.size());

            return snapshotWriteService.writeSnapshot(channelName, currentPosition, stateRepository);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            stateRepository.clear();
        }
    }

}

