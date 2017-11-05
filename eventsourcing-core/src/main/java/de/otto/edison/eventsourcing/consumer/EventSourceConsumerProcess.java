package de.otto.edison.eventsourcing.consumer;


import org.slf4j.Logger;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.slf4j.LoggerFactory.getLogger;

public class EventSourceConsumerProcess {

    // Siehe https://programtalk.com/java/executorservice-not-shutting-down/

    private static final Logger LOG = getLogger(EventSourceConsumerProcess.class);
    public static final String THREAD_NAME_PREFIX = "edison-eventsourcing-consumer-";

    private final ExecutorService executorService;
    private final AtomicBoolean stopThread = new AtomicBoolean(false);
    private final EventConsumer<Object> eventConsumer;
    private final EventSource<Object> eventSource;

    @SuppressWarnings("unchecked")
    public <T> EventSourceConsumerProcess(final EventSource<T> eventSource,
                                          final EventConsumer<T> eventConsumer) {
        final ThreadFactory threadFactory = new CustomizableThreadFactory(THREAD_NAME_PREFIX);
        executorService = newSingleThreadExecutor(threadFactory);
        this.eventSource = (EventSource<Object>) eventSource;
        this.eventConsumer = (EventConsumer<Object>) eventConsumer;
    }

    @PostConstruct
    public void init() {
        executorService.execute(() -> {
            try {
                LOG.info("Starting...");
                eventSource.consumeAll(ignore -> stopThread.get(), eventConsumer);
            } catch (Exception e) {
                LOG.error("Starting failed: " + e.getMessage(), e);
            }
        });

    }

    @PreDestroy
    public void shutdown() {
        LOG.info("Shutting down...");
        this.stopThread.set(true);

        if (executorService != null) {
            try {
                // TODO: configure timeout for shutting down event consumers
                executorService.awaitTermination(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        LOG.info("...done.");
    }
}
