package de.otto.synapse.endpoint.receiver;


import de.otto.synapse.channel.ChannelPosition;
import org.slf4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.SmartLifecycle;

import static org.slf4j.LoggerFactory.getLogger;

public class MessageLogConsumerContainer implements SmartLifecycle {

    private static final Logger LOG = getLogger(MessageLogConsumerContainer.class);

    private final MessageLogReceiverEndpoint endpoint;
    private final ChannelPosition startFrom;

    private volatile boolean running = false;

    public MessageLogConsumerContainer(final String messageLogBeanName,
                                       final ChannelPosition startFrom,
                                       final ApplicationContext applicationContext) {
        this.endpoint = applicationContext.getBean(messageLogBeanName, MessageLogReceiverEndpoint.class);
        this.startFrom = startFrom;
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
        LOG.info("Initializing MessageLogConsumerContainer with {} message logs", endpoint.getChannelName());
        running = true;
            try {
                LOG.info("Starting message log {}...", endpoint.getChannelName());
                endpoint.consume(startFrom);
            } catch (Exception e) {
                LOG.error("Starting message log failed: " + e.getMessage(), e);
                stop();
            }
    }

    @Override
    public void stop() {
        LOG.info("Shutting down...");
        endpoint.stop();
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
