package de.otto.edison.eventsourcing.consumer;


import org.slf4j.Logger;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.slf4j.LoggerFactory.getLogger;

public class EventSourceConsumerProcess {

    // Siehe https://programtalk.com/java/executorservice-not-shutting-down/

    private static final Logger LOG = getLogger(EventSourceConsumerProcess.class);
    public static final String THREAD_NAME_PREFIX = "edison-eventsourcing-consumer-";

    private final AtomicBoolean stopThread = new AtomicBoolean(false);

    private final ExecutorService executorService;
    private final Map<EventSource, EventConsumer> eventSourceWithConsumer = new ConcurrentHashMap<>();

    public EventSourceConsumerProcess(final List<EventSource> eventSources,
                                      final List<EventConsumer> eventConsumers) {
        matchEventConsumersWithEventSourcesByStreamName(eventSources, eventConsumers);
        if (eventSourceWithConsumer.size() > 0) {
            final ThreadFactory threadFactory = new CustomizableThreadFactory(THREAD_NAME_PREFIX);
            executorService = newFixedThreadPool(eventSourceWithConsumer.size(), threadFactory);
        } else {
            executorService = null;
        }
    }

    private void matchEventConsumersWithEventSourcesByStreamName(List<EventSource> eventSources, List<EventConsumer> eventConsumers) {
        eventConsumers.forEach(consumer ->
                eventSources
                .stream()
                .filter(es -> es.name().equals(consumer.streamName()))
                .findAny()
                .ifPresent(eventSource -> eventSourceWithConsumer.put(eventSource, consumer)));
    }

    @PostConstruct
    @SuppressWarnings("unchecked")
    public void init() {
        LOG.info("Initializing EventSourceConsumerProcess...");
        eventSourceWithConsumer.forEach((eventSource, eventConsumer) -> executorService.submit(() -> {
            try {
                LOG.info("Starting {}...", eventConsumer.streamName());
                eventSource.consumeAll(ignore -> stopThread.get(), eventConsumer);
            } catch (Exception e) {
                LOG.error("Starting failed: " + e.getMessage(), e);
            }
        }));
    }

    @PreDestroy
    public void shutdown() {
        LOG.info("Shutting down...");
        this.stopThread.set(true);
        if (executorService != null) {
            try {
                executorService.shutdown();
                executorService.awaitTermination(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        LOG.info("...done.");
    }
}
