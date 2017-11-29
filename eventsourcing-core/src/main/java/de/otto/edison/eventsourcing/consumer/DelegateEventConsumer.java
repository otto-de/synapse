package de.otto.edison.eventsourcing.consumer;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.LinkedListMultimap;
import de.otto.edison.eventsourcing.annotation.EventSourceMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.regex.Pattern;

class DelegateEventConsumer implements EventConsumer<String> {

    private static final Logger LOG = LoggerFactory.getLogger(DelegateEventConsumer.class);

    private final EventSourceMapping.ConsumerMapping consumerMapping;
    private final ObjectMapper objectMapper;
    private final LinkedListMultimap<Pattern, EventConsumer> mapPatternToEventSource = LinkedListMultimap.create();

    DelegateEventConsumer(EventSourceMapping.ConsumerMapping consumerMapping, ObjectMapper objectMapper) {
        this.consumerMapping = consumerMapping;
        this.objectMapper = objectMapper;
        assertSameStreamNameForAllConsumers();
        assertConsumersAreRegistered();
        registerPatternMatcher();
    }

    private void assertConsumersAreRegistered() {
        Set<String> keyPatterns = consumerMapping.getKeyPatterns();

        if (keyPatterns.isEmpty()) {
            throw new IllegalArgumentException("No consumers registered.");
        }

        keyPatterns.forEach(keyPattern -> {
            List<EventConsumer> consumerForKeyPattern = consumerMapping.getConsumersForKeyPattern(keyPattern);
            if (consumerForKeyPattern == null || consumerForKeyPattern.isEmpty()) {
                throw new IllegalArgumentException(String.format("No consumer for key pattern %s registered!", keyPattern));
            }
        });
    }

    private void registerPatternMatcher() {
        consumerMapping.getKeyPatterns()
                .forEach(keyPattern -> mapPatternToEventSource.putAll(
                        Pattern.compile(keyPattern),
                        consumerMapping.getConsumersForKeyPattern(keyPattern)));
    }

    private void assertSameStreamNameForAllConsumers() {
        long count = mapPatternToEventSource.values().stream()
                .map(EventConsumer::streamName)
                .distinct()
                .count();
        if (count > 1) {
            throw new IllegalArgumentException("event consumers must have same event stream name");
        }
    }

    @Override
    public String streamName() {
        return consumerMapping.getStreamName();
    }

    @Override
    public Consumer<Event<String>> consumerFunction() {
        return this::accept;
    }

    @SuppressWarnings("unchecked")
    private void accept(Event<String> event) {
        mapPatternToEventSource.keySet().stream()
                .filter(keyPattern -> matchesEventKey(event, keyPattern))
                .forEach(keyPattern -> {
                    List<EventConsumer> eventConsumers = mapPatternToEventSource.get(keyPattern);
                    for (EventConsumer eventConsumer : eventConsumers) {
                        try {
                            Class<?> payload = consumerMapping.getPayloadForEventConsumer(eventConsumer).get();
                            Event<?> parsedEvent = parseEvent(event, payload);
                            eventConsumer.consumerFunction().accept(parsedEvent);
                        } catch (Exception e) {
                            LOG.error("", e);
                        }
                    }
                });
    }

    private Event<?> parseEvent(Event<String> event, Class<?> payloadType) {
        try {
            Object parsedPayload = objectMapper.readValue(event.payload(), payloadType);
            return new Event<>(event.key(), parsedPayload, event.sequenceNumber(), event.arrivalTimestamp(), event.durationBehind().orElse(null));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean matchesEventKey(Event<String> event, Pattern keyPattern) {
        return keyPattern.matcher(event.key()).matches();
    }

}