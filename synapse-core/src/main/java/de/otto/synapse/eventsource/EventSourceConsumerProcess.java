package de.otto.synapse.eventsource;


import org.slf4j.Logger;
import org.springframework.context.SmartLifecycle;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

import java.util.List;
import java.util.concurrent.ThreadFactory;

import static org.slf4j.LoggerFactory.getLogger;

public class EventSourceConsumerProcess implements SmartLifecycle {

    private static final Logger LOG = getLogger(EventSourceConsumerProcess.class);
    private static final String THREAD_NAME_PREFIX = "synapse-consumer-";

    private final List<EventSource> eventSources;

    private volatile boolean running = false;

    public EventSourceConsumerProcess(final List<EventSource> eventSources) {
        this.eventSources = eventSources;
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
        final int eventSourceCount = eventSources != null ? eventSources.size() : 0;
        if (eventSourceCount > 0) {
            LOG.info("Initializing EventSourceConsumerProcess with {} EventSources", eventSourceCount);
            running = true;
            final ThreadFactory threadFactory = new CustomizableThreadFactory(THREAD_NAME_PREFIX);
            eventSources.forEach(eventSource -> {
                try {
                    LOG.info("Starting {}...", eventSource.getChannelName());
                    eventSource.consume();
                } catch (Exception e) {
                    LOG.error("Starting failed: " + e.getMessage(), e);
                    stop();
                }
            });
        } else {
            LOG.warn("Did not find any EventSource instances to execute");
            running = false;
        }
    }

    @Override
    public void stop() {
        LOG.info("Shutting down...");
        eventSources.forEach(EventSource::stop);
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
