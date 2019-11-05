package de.otto.synapse.endpoint.receiver;


import org.slf4j.Logger;
import org.springframework.context.SmartLifecycle;

import java.util.List;

import static org.slf4j.LoggerFactory.getLogger;

public class MessageQueueConsumerProcess implements SmartLifecycle {

    private static final Logger LOG = getLogger(MessageQueueConsumerProcess.class);

    private final List<MessageQueueReceiverEndpoint> messageQueueReceiverEndpoints;

    private volatile boolean running = false;

    public MessageQueueConsumerProcess(final List<MessageQueueReceiverEndpoint> messageQueueReceiverEndpoints) {
        this.messageQueueReceiverEndpoints = messageQueueReceiverEndpoints;
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public void stop(final Runnable callback) {
        stop();
        callback.run();
    }

    @Override
    public int getPhase() {
        return 0;
    }

    @Override
    public void start() {
        final int receiverCount = messageQueueReceiverEndpoints != null ? messageQueueReceiverEndpoints.size() : 0;
        if (receiverCount > 0) {
            LOG.info("Initializing MessageQueueConsumerProcess with {} message queues", receiverCount);
            running = true;
            messageQueueReceiverEndpoints.forEach(endpoint -> {
                try {
                    LOG.info("Starting {}...", endpoint.getChannelName());
                    endpoint.consume();
                } catch (Exception e) {
                    LOG.error("Starting failed: " + e.getMessage(), e);
                    stop();
                }
            });
        } else {
            LOG.warn("Did not find any MessageQueueReceiverEndpoint instance to execute");
            running = false;
        }
    }

    @Override
    public void stop() {
        LOG.info("Shutting down...");
        messageQueueReceiverEndpoints.forEach(MessageQueueReceiverEndpoint::stop);
        running = false;
        LOG.info("...done.");
    }

    /**
     * Check whether this component is currently running.
     * <p>In the case of a container, this will return {@code true} only if <i>all</i>
     * components that apply are currently running.
     *
     * @return whether the component is currently running
     */
    @Override
    public boolean isRunning() {
        return running;
    }
}
