package de.otto.synapse.eventsource.aws;

import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.consumer.MessageDispatcher;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpoint;
import de.otto.synapse.eventsource.EventSource;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.util.Objects;

public class CompactedKinesisEventSource implements EventSource {

    private final EventSource snapshotEventSource;
    private final EventSource kinesisEventSource;
    private final String channelName;

    public CompactedKinesisEventSource(EventSource snapshotEventSource,
                                       EventSource kinesisEventSource) {
        Objects.requireNonNull(snapshotEventSource, "snapshot event source must not be null");
        Objects.requireNonNull(kinesisEventSource, "kinesis event source must not be null");
        if (!snapshotEventSource.getChannelName().equals(kinesisEventSource.getChannelName())) {
            throw new IllegalArgumentException(String.format(
                    "given event sources must have same stream name, but was: '%s' and '%s'",
                    snapshotEventSource.getChannelName(), kinesisEventSource.getChannelName()));
        }
        this.snapshotEventSource = snapshotEventSource;
        this.kinesisEventSource = kinesisEventSource;
        this.channelName = kinesisEventSource.getChannelName();
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
    @Nonnull
    @Override
    public MessageDispatcher getMessageDispatcher() {
        return snapshotEventSource.getMessageDispatcher();
    }

    @Nonnull
    @Override
    public MessageLogReceiverEndpoint getMessageLogReceiverEndpoint() {
        return kinesisEventSource.getMessageLogReceiverEndpoint();
    }

    @Override
    public String getChannelName() {
        return channelName;
    }

    @Nonnull
    @Override
    public ChannelPosition consumeUntil(@Nonnull final ChannelPosition startFrom,
                                        @Nonnull final Instant until) {
        final ChannelPosition channelPosition = snapshotEventSource.consume();
        return kinesisEventSource.consumeUntil(channelPosition, until);
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
