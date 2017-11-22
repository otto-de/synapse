package de.otto.edison.eventsourcing.consumer;


import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.function.Consumer;

class DelegateEventConsumer<T> implements EventConsumer<T> {

    Logger LOG = LoggerFactory.getLogger(DelegateEventConsumer.class);

    private ImmutableList<EventConsumer> eventConsumers;
    private String streamName = null;

    public DelegateEventConsumer(Collection<EventConsumer> eventConsumers) {
        if (eventConsumers.isEmpty()) {
            throw new IllegalArgumentException("list of event consumers must not be empty");
        }
        this.eventConsumers = ImmutableList.copyOf(eventConsumers);
        assertSameStreamNameForAllConsumers();
        this.streamName = this.eventConsumers.get(0).streamName();
    }

    private void assertSameStreamNameForAllConsumers() {
        long count = eventConsumers.stream()
                .map(EventConsumer::streamName)
                .distinct()
                .count();
        if (count > 1) {
            throw new IllegalArgumentException("event consumers must have same event stream name");
        }
    }

    @Override
    public String streamName() {
        return streamName;
    }

    @Override
    public Consumer<Event<T>> consumerFunction() {
        return this::accept;
    }

    private void accept(Event<T> event) {
        eventConsumers.forEach(eventConsumer -> {
            try {
                eventConsumer.consumerFunction().accept(event);
            } catch (Exception e) {
                LOG.error("error in consuming event");
            }
        });
    }

}