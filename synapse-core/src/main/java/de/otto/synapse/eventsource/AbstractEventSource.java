package de.otto.synapse.eventsource;

import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.consumer.MessageDispatcher;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpoint;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.slf4j.LoggerFactory.getLogger;

public abstract class AbstractEventSource implements EventSource {

    private static final Logger LOG = getLogger(AbstractEventSource.class);

    private final String name;
    private final MessageLogReceiverEndpoint messageLog;
    private final AtomicBoolean stopping = new AtomicBoolean(false);

    public AbstractEventSource(final String name,
                               final MessageLogReceiverEndpoint messageLog) {
        this.name = name;
        this.messageLog = messageLog;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getChannelName() {
        return messageLog.getChannelName();
    }

    @Override
    public void stop() {
        LOG.info("Stopping EventSource {}", name);
        stopping.set(true);
        messageLog.stop();
    }

    @Override
    public boolean isStopping() {
        return stopping.get();
    }

    /**
     * Registers a new EventConsumer at the EventSource.
     * <p>
     * {@link MessageConsumer consumers} have to be thread safe as it may be called from multiple threads
     * (e.g. for kinesis streams there is one thread per shard)
     *
     * @param messageConsumer the EventConsumer that is registered at the EventSource
     */
    @Override
    public void register(final MessageConsumer<?> messageConsumer) {
        messageLog.register(messageConsumer);
    }

    /**
     * Returns the MessageDispatcher used to register {@link MessageConsumer consumers} at the EventSource.
     *
     * @return MessageDispatcher
     */
    @Nonnull
    public MessageDispatcher getMessageDispatcher() {
        return messageLog.getMessageDispatcher();
    }

    @Nonnull
    public MessageLogReceiverEndpoint getMessageLogReceiverEndpoint() {
        return messageLog;
    }

    @Nonnull
    @Override
    public ChannelPosition consumeUntil(final @Nonnull ChannelPosition startFrom,
                                        final @Nonnull Instant until) {
        return messageLog.consumeUntil(startFrom, until);
    }

}
