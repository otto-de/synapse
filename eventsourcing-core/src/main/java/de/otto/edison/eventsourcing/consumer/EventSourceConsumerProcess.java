package de.otto.edison.eventsourcing.consumer;


import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

import javax.annotation.PreDestroy;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.slf4j.LoggerFactory.getLogger;

public class EventSourceConsumerProcess {

    private static final Logger LOG = getLogger(EventSourceConsumerProcess.class);
    private static final String THREAD_NAME_PREFIX = "edison-eventsourcing-consumer-";

    private final AtomicBoolean stopThread = new AtomicBoolean(false);

    private ExecutorService executorService;
    @Autowired(required = false)
    private List<EventSource> eventSources;

    public EventSourceConsumerProcess() {
    }

    EventSourceConsumerProcess(final List<EventSource> eventSources) {
        this.eventSources = eventSources;
    }

    @EventListener
    public void handleContextRefresh(ContextRefreshedEvent event) {
        init();
    }

    @SuppressWarnings("unchecked")
    public void init() {
        LOG.info("Initializing EventSourceConsumerProcess...");

        int eventSourceCount = eventSources != null ? eventSources.size() : 0;
        if (eventSourceCount > 0) {
            final ThreadFactory threadFactory = new CustomizableThreadFactory(THREAD_NAME_PREFIX);
            executorService = newFixedThreadPool(eventSourceCount, threadFactory);
            eventSources.forEach(eventSource -> executorService.submit(() -> {
                try {
                    LOG.info("Starting {}...", eventSource.getStreamName());
                    eventSource.consumeAll(ignore -> stopThread.get());
                } catch (Exception e) {
                    LOG.error("Starting failed: " + e.getMessage(), e);
                }
            }));
        } else {
            LOG.warn("Did not find any EventSource instances to execute");
            executorService = null;
        }
    }

    @PreDestroy
    public void shutdown() {
        LOG.info("Shutting down...");
        this.stopThread.set(true);
        if (executorService != null) {
            try {
                executorService.shutdownNow();
                executorService.awaitTermination(2, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        LOG.info("...done.");
    }

}
