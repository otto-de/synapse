package de.otto.edison.eventsourcing.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import static java.util.Collections.synchronizedList;
import static java.util.Collections.unmodifiableList;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Helper class used to encode and forward an Event with JSON payload into the target-types expected by
 * EventConsumer instances.
 */
public class EventConsumers {

    private static final Logger LOG = getLogger(EventConsumers.class);

    private final List<EventConsumer<?>> eventConsumers;
    private final ObjectMapper objectMapper;

    public EventConsumers(final ObjectMapper objectMapper) {
        this.eventConsumers = synchronizedList(new ArrayList<>());
        this.objectMapper = objectMapper;
    }

    public EventConsumers(final ObjectMapper objectMapper, final List<EventConsumer<?>> eventConsumers) {
        this.eventConsumers = synchronizedList(new ArrayList<>(eventConsumers));
        this.objectMapper = objectMapper;
        assertSameStreamNameForAllConsumers();
    }

    public void add(final EventConsumer<?> eventConsumer) {
        this.eventConsumers.add(eventConsumer);
        assertSameStreamNameForAllConsumers();
    }

    public List<EventConsumer<?>> getAll() {
        return unmodifiableList(eventConsumers);
    }

    @SuppressWarnings({"unchecked", "raw"})
    public void encodeAndSend(final Event<String> event) {
        eventConsumers
                .stream()
                .filter(consumer -> matchesEventKey(event, consumer.keyPattern()))
                .forEach((EventConsumer consumer) -> {
                    try {
                        final Class<?> payloadType = consumer.payloadType();
                        if (payloadType.equals(String.class)) {
                            consumer.accept(event);
                        } else {
                            final Object payload = objectMapper.readValue(event.payload(), payloadType);
                            final Event<?> tEvent = Event.event(event.key(), payload, event.sequenceNumber(), event.arrivalTimestamp(), event.durationBehind().orElse(null));
                            consumer.accept(tEvent);
                        }
                    } catch (final Exception e) {
                        LOG.error(e.getMessage(), e);
                    }
                });
    }

    private boolean matchesEventKey(Event<String> event, Pattern keyPattern) {
        return keyPattern.matcher(event.key()).matches();
    }

    private void assertSameStreamNameForAllConsumers() {
        long count = eventConsumers
                .stream()
                .map(EventConsumer::streamName)
                .distinct()
                .count();
        if (count > 1) {
            throw new IllegalArgumentException("Event consumers must have same event stream name");
        }
    }

}
