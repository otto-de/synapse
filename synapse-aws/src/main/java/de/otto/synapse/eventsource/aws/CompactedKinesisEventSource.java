package de.otto.synapse.eventsource.aws;

import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.consumer.MessageDispatcher;
import de.otto.synapse.eventsource.EventSource;
import de.otto.synapse.message.Message;

import java.util.Objects;
import java.util.function.Predicate;

public class CompactedKinesisEventSource implements EventSource {

    private final EventSource snapshotEventSource;
    private final EventSource kinesisEventSource;
    private final String streamName;

    public CompactedKinesisEventSource(EventSource snapshotEventSource,
                                       EventSource kinesisEventSource) {
        Objects.requireNonNull(snapshotEventSource, "snapshot event source must not be null");
        Objects.requireNonNull(kinesisEventSource, "kinesis event source must not be null");
        if (!snapshotEventSource.getStreamName().equals(kinesisEventSource.getStreamName())) {
            throw new IllegalArgumentException(String.format(
                    "given event sources must have same stream name, but was: '%s' and '%s'",
                    snapshotEventSource.getStreamName(), kinesisEventSource.getStreamName()));
        }
        this.snapshotEventSource = snapshotEventSource;
        this.kinesisEventSource = kinesisEventSource;
        this.streamName = kinesisEventSource.getStreamName();
    }

    /**
     * Registers a new EventConsumer at the EventSource.
     * <p>
     * {@link MessageConsumer consumers} have to be thread safe as it may be called from multiple threads
     * (e.g. for kinesis streams there is one thread per shard)
     *
     * @param messageConsumer registered EventConsumer
     */
    @Override
    public void register(final MessageConsumer<?> messageConsumer) {
        snapshotEventSource.register(messageConsumer);
        kinesisEventSource.register(messageConsumer);
    }

    @Override
    public String getName() {
        return kinesisEventSource.getName();
    }

    /**
     * Returns the list of registered EventConsumers.
     *
     * @return list of registered EventConsumers
     */
    @Override
    public MessageDispatcher getMessageDispatcher() {
        return snapshotEventSource.getMessageDispatcher();
    }

    @Override
    public String getStreamName() {
        return streamName;
    }

    @Override
    public ChannelPosition consumeAll(ChannelPosition startFrom, Predicate<Message<?>> stopCondition) {
        Predicate<Message<?>> neverStop = e -> false;
        final ChannelPosition channelPosition = snapshotEventSource.consumeAll(neverStop);
        return kinesisEventSource.consumeAll(channelPosition, stopCondition);
    }

    @Override
    public void stop() {
        kinesisEventSource.stop();
        snapshotEventSource.stop();
    }

    @Override
    public boolean isStopping() {
        return kinesisEventSource.isStopping() || snapshotEventSource.isStopping();
    }
}
