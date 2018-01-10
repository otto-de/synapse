package de.otto.edison.eventsourcing.consumer;


import org.slf4j.Logger;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.Lifecycle;
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


    /**
     * Returns {@code true} if this {@code Lifecycle} component should get
     * started automatically by the container at the time that the containing
     * {@link ApplicationContext} gets refreshed.
     * <p>A value of {@code false} indicates that the component is intended to
     * be started through an explicit {@link #start()} call instead, analogous
     * to a plain {@link Lifecycle} implementation.
     *
     * @see #start()
     * @see #getPhase()
     * @see LifecycleProcessor#onRefresh()
     * @see ConfigurableApplicationContext#refresh()
     */
    @Override
    public boolean isAutoStartup() {
        return true;
    }

    /**
     * Indicates that a Lifecycle component must stop if it is currently running.
     * <p>The provided callback is used by the {@link LifecycleProcessor} to support
     * an ordered, and potentially concurrent, shutdown of all components having a
     * common shutdown order value. The callback <b>must</b> be executed after
     * the {@code SmartLifecycle} component does indeed stop.
     * <p>The {@link LifecycleProcessor} will call <i>only</i> this variant of the
     * {@code stop} method; i.e. {@link Lifecycle#stop()} will not be called for
     * {@code SmartLifecycle} implementations unless explicitly delegated to within
     * the implementation of this method.
     *
     * @param callback
     * @see #stop()
     * @see #getPhase()
     */
    @Override
    public void stop(final Runnable callback) {
        stop();
        callback.run();
    }

    /**
     * Return the phase value of this object.
     */
    @Override
    public int getPhase() {
        return 0;
    }

    /**
     * Start this component.
     * <p>Should not throw an exception if the component is already running.
     * <p>In the case of a container, this will propagate the start signal to all
     * components that apply.
     *
     * @see SmartLifecycle#isAutoStartup()
     */
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
                }
            }));
        } else {
            LOG.warn("Did not find any EventSource instances to execute");
            executorService = null;
        }
    }

    /**
     * Stop this component, typically in a synchronous fashion, such that the component is
     * fully stopped upon return of this method. Consider implementing {@link SmartLifecycle}
     * and its {@code stop(Runnable)} variant when asynchronous stop behavior is necessary.
     * <p>Note that this stop notification is not guaranteed to come before destruction: On
     * regular shutdown, {@code Lifecycle} beans will first receive a stop notification before
     * the general destruction callbacks are being propagated; however, on hot refresh during a
     * context's lifetime or on aborted refresh attempts, only destroy methods will be called.
     * <p>Should not throw an exception if the component isn't started yet.
     * <p>In the case of a container, this will propagate the stop signal to all components
     * that apply.
     *
     * @see SmartLifecycle#stop(Runnable)
     * @see DisposableBean#destroy()
     */
    @Override
    public void stop() {
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
