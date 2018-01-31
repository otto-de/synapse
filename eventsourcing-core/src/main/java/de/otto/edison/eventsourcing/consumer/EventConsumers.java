package de.otto.edison.eventsourcing.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.eventsourcing.event.Event;
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
    }

    public void add(final EventConsumer<?> eventConsumer) {
        this.eventConsumers.add(eventConsumer);
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
                            Object payload = null;
                            if (event.getEventBody().getPayload() != null) {
                                payload = objectMapper.readValue(event.getEventBody().getPayload(), payloadType);
                            }
                            final Event<?> tEvent = Event.event(event.getEventBody().getKey(), payload, event.getSequenceNumber(), event.getArrivalTimestamp(), event.getDurationBehind().orElse(null));
                            consumer.accept(tEvent);
                        }
                    } catch (final Exception e) {
                        LOG.error(e.getMessage(), e);
                    }
                });
    }

    private boolean matchesEventKey(Event<String> event, Pattern keyPattern) {
        return keyPattern.matcher(event.getEventBody().getKey()).matches();
    }

}
