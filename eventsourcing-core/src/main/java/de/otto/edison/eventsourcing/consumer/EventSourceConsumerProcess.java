package de.otto.edison.eventsourcing.consumer;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import de.otto.edison.eventsourcing.annotation.EventSourceMapping;
import org.slf4j.Logger;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

import javax.annotation.PreDestroy;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.slf4j.LoggerFactory.getLogger;

public class EventSourceConsumerProcess {

    // Siehe https://programtalk.com/java/executorservice-not-shutting-down/

    private static final Logger LOG = getLogger(EventSourceConsumerProcess.class);
    private static final String THREAD_NAME_PREFIX = "edison-eventsourcing-consumer-";

    private final AtomicBoolean stopThread = new AtomicBoolean(false);

    private final ExecutorService executorService;
    private final EventSourceMapping eventSourceMapping;
    private final ObjectMapper objectMapper;

    public EventSourceConsumerProcess(final EventSourceMapping eventSourceMapping) {
        this(eventSourceMapping, new ObjectMapper().registerModule(new JavaTimeModule()));
    }

    public EventSourceConsumerProcess(final EventSourceMapping eventSourceMapping,
                                      final ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        this.eventSourceMapping = eventSourceMapping;

        int eventSourceCount = eventSourceMapping.getEventSources().size();
        if (eventSourceCount > 0) {
            final ThreadFactory threadFactory = new CustomizableThreadFactory(THREAD_NAME_PREFIX);
            executorService = newFixedThreadPool(eventSourceCount, threadFactory);
        } else {
            executorService = null;
        }
    }


    @EventListener
    public void handleContextRefresh(ContextRefreshedEvent event) {
        init();
    }

    @SuppressWarnings("unchecked")
    public void init() {
        LOG.info("Initializing EventSourceConsumerProcess...");
        eventSourceMapping.getEventSources()
                .forEach(eventSource -> executorService.submit(() -> {
                            try {
                                LOG.info("Starting {}...", eventSource.getStreamName());
                                EventSourceMapping.ConsumerMapping consumerMapping = eventSourceMapping.getConsumerMapping(eventSource);
                                DelegateEventConsumer delegateEventConsumer = new DelegateEventConsumer(consumerMapping, objectMapper);
                                eventSource.consumeAll(ignore -> stopThread.get(), delegateEventConsumer);
                            } catch (Exception e) {
                                LOG.error("Starting failed: " + e.getMessage(), e);
                            }
                        }
                ));
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
