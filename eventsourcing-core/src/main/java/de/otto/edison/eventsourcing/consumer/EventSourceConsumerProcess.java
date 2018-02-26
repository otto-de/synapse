package de.otto.edison.eventsourcing.consumer;


import org.slf4j.Logger;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.slf4j.LoggerFactory.getLogger;

public class EventSourceConsumerProcess implements SmartLifecycle {

    private static final Logger LOG = getLogger(EventSourceConsumerProcess.class);
    private static final String THREAD_NAME_PREFIX = "edison-eventsourcing-consumer-";

    private final AtomicBoolean stopThread = new AtomicBoolean(false);
    private final List<EventSource> eventSources;

    private volatile ExecutorService executorService;
    private volatile boolean running = false;

    public EventSourceConsumerProcess(final List<EventSource> eventSources) {
        this.eventSources = eventSources;
    }

    @EventListener
    public void handleContextRefresh(ContextRefreshedEvent event) {
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
            executorService = newFixedThreadPool(eventSourceCount, threadFactory);
            eventSources.forEach(eventSource -> executorService.submit(() -> {
                try {
                    LOG.info("Starting {}...", eventSource.getStreamName());
                    eventSource.consumeAll(ignore -> stopThread.get());
                } catch (Exception e) {
                    LOG.error("Starting failed: " + e.getMessage(), e);
                    eventSource.stop();
                }
            }));
        } else {
            LOG.warn("Did not find any EventSource instances to execute");
            executorService = null;
        }
    }

    @Override
    public void stop() {
        LOG.info("Shutting down...");
        this.stopThread.set(true);
        if (executorService != null) {
            try {
                eventSources.forEach(EventSource::stop);
                executorService.shutdownNow();
                executorService.awaitTermination(2, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
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
