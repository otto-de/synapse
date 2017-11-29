package de.otto.edison.eventsourcing.annotation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import de.otto.edison.eventsourcing.consumer.EventConsumer;
import de.otto.edison.eventsourcing.consumer.EventSource;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class EventSourceMapping {

    private Map<EventSource<String>, ConsumerMapping> mapEventSourceToMapping = new ConcurrentHashMap<>();

    public Set<EventSource<String>> getEventSources() {
        return ImmutableSet.copyOf(mapEventSourceToMapping.keySet());
    }

    public ConsumerMapping getConsumerMapping(EventSource<String> eventSource) {
        ConsumerMapping consumerMapping = mapEventSourceToMapping.get(eventSource);
        if (consumerMapping == null) {
            consumerMapping = new ConsumerMapping(eventSource.getStreamName());
            mapEventSourceToMapping.put(eventSource, consumerMapping);
        }
        return consumerMapping;
    }



    public static class ConsumerMapping {
        private final Multimap<String, EventConsumer> mapKeyToConsumer = LinkedHashMultimap.create();
        private final Map<EventConsumer, Class<?>> mapKeyToPayload = new ConcurrentHashMap<>();
        private final String streamName;

        public ConsumerMapping(String streamName) {
            this.streamName = streamName;
        }

        public String getStreamName() {
            return streamName;
        }

        public Set<String> getKeyPatterns() {
            return ImmutableSet.copyOf(mapKeyToConsumer.keySet());
        }

        public List<EventConsumer> getConsumerForKeyPattern(String keyPattern) {
            return ImmutableList.copyOf(mapKeyToConsumer.get(keyPattern));
        }

        public Optional<Class<?>> getPayloadForEventConsumer(EventConsumer eventConsumer) {
            return Optional.ofNullable(mapKeyToPayload.get(eventConsumer));
        }

        public void addConsumerAndPayloadForKeyPattern(String keyPattern, EventConsumer eventConsumer, Class<?> payload) {
            if (!streamName.equals(eventConsumer.streamName())) {
                throw new IllegalArgumentException("event source and event consumer must have same stream name");
            }
            mapKeyToConsumer.put(keyPattern, eventConsumer);
            mapKeyToPayload.put(eventConsumer, payload);
        }
    }

}
