package de.otto.edison.eventsourcing.consumer;


import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.regex.Pattern;

class DelegateEventConsumer<T> implements EventConsumer<T> {

    private static final Logger LOG = LoggerFactory.getLogger(DelegateEventConsumer.class);

    private final ImmutableList<EventConsumer> eventConsumers;
    private final Map<EventConsumer, Pattern> eventConsumerMatcherMap = new ConcurrentHashMap<>();

    private String streamName = null;

    DelegateEventConsumer(Collection<EventConsumer> eventConsumers) {
        if (eventConsumers.isEmpty()) {
            throw new IllegalArgumentException("list of event consumers must not be empty");
        }
        this.eventConsumers = ImmutableList.copyOf(eventConsumers);
        this.streamName = this.eventConsumers.get(0).streamName();
        assertSameStreamNameForAllConsumers();
        registerPatternMatcher();
    }

    @Override
    public String getKeyPattern() {
        return ".*";
    }

    private void registerPatternMatcher() {
        eventConsumers.forEach(eventConsumer -> eventConsumerMatcherMap.put(eventConsumer, Pattern.compile(eventConsumer.getKeyPattern())));
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

    @SuppressWarnings("unchecked")
    private void accept(Event<T> event) {
        eventConsumers.stream()
                .filter(eventConsumer -> matchesEventKey(eventConsumer, event))
                .forEach(eventConsumer -> {
                    try {
                        eventConsumer.consumerFunction().accept(event);
                    } catch (Exception e) {
                        LOG.error("error in consuming event");
                    }
                });
    }

    private boolean matchesEventKey(EventConsumer eventConsumer, Event<T> event) {
        return eventConsumerMatcherMap.get(eventConsumer).matcher(event.key()).matches();
    }

}