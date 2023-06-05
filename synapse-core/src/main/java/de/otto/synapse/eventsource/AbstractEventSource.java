package de.otto.synapse.eventsource;

import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.consumer.MessageDispatcher;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpoint;
import jakarta.annotation.Nonnull;
import org.slf4j.Logger;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.slf4j.LoggerFactory.getLogger;

public abstract class AbstractEventSource implements EventSource {

    private static final Logger LOG = getLogger(AbstractEventSource.class);

    private final MessageLogReceiverEndpoint messageLog;
    private final AtomicBoolean stopping = new AtomicBoolean(false);

    public AbstractEventSource(final MessageLogReceiverEndpoint messageLog) {
        this.messageLog = messageLog;
    }

    @Override
    public String getChannelName() {
        return messageLog.getChannelName();
    }

    @Override
    public void stop() {
        LOG.info("Stopping EventSource {}", getChannelName());
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

}
