package de.otto.edison.eventsourcing.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.eventsourcing.message.Message;
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
    public void encodeAndSend(final Message<String> message) {
        eventConsumers
                .stream()
                .filter(consumer -> matchesEventKey(message, consumer.keyPattern()))
                .forEach((EventConsumer consumer) -> {
                    try {
                        final Class<?> payloadType = consumer.payloadType();
                        if (payloadType.equals(String.class)) {
                            consumer.accept(message);
                        } else {
                            Object payload = null;
                            if (message.getPayload() != null) {
                                payload = objectMapper.readValue(message.getPayload(), payloadType);
                            }
                            final Message<?> tMessage = Message.message(message.getKey(), message.getHeader(), payload);
                            consumer.accept(tMessage);
                        }
                    } catch (final Exception e) {
                        LOG.error(e.getMessage(), e);
                    }
                });
    }

    private boolean matchesEventKey(Message<String> message, Pattern keyPattern) {
        return keyPattern.matcher(message.getKey()).matches();
    }

}
