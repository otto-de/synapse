package de.otto.edison.eventsourcing.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.eventsourcing.event.Event;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;

import static de.otto.edison.eventsourcing.consumer.TestEventConsumer.testEventConsumer;
import static de.otto.edison.eventsourcing.event.Event.event;
import static java.util.Arrays.asList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class EventConsumersTest {

    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    @SuppressWarnings("unchecked")
    public void shouldDelegateEventsToAllConsumers() {
        // given
        TestEventConsumer<Object> eventConsumerA = spy(testEventConsumer(".*", Object.class));
        TestEventConsumer<Object> eventConsumerB = spy(testEventConsumer(".*", Object.class));
        TestEventConsumer<Object> eventConsumerC = spy(testEventConsumer(".*", Object.class));

        EventConsumers eventConsumers = new EventConsumers(OBJECT_MAPPER);
        eventConsumers.add(eventConsumerA);
        eventConsumers.add(eventConsumerB);
        eventConsumers.add(eventConsumerC);

        // when
        Event<String> someEvent = event("someKey", "{}", "0", Instant.now(), Duration.ZERO);
        eventConsumers.encodeAndSend(someEvent);

        // then
        verify(eventConsumerA).accept(any(Event.class));
        verify(eventConsumerB).accept(any(Event.class));
        verify(eventConsumerC).accept(any(Event.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldDelegateEventsToSpecificConsumersForEventKey() {
        // given

        TestEventConsumer<Apple> eventConsumerApple = spy(testEventConsumer("apple.*", Apple.class));
        TestEventConsumer<Banana> eventConsumerBanana = spy(testEventConsumer("banana.*", Banana.class));
        TestEventConsumer<Cherry> eventConsumerCherry = spy(testEventConsumer("cherry.*", Cherry.class));

        EventConsumers eventConsumers = new EventConsumers(OBJECT_MAPPER, asList(eventConsumerApple, eventConsumerBanana, eventConsumerCherry));

        // when
        Event<String> someAppleEvent = event("apple.123", "{}", "0", Instant.now(), Duration.ZERO);
        Event<String> someBananaEvent = event("banana.456", "{}", "0", Instant.now(), Duration.ZERO);
        eventConsumers.encodeAndSend(someAppleEvent);
        eventConsumers.encodeAndSend(someBananaEvent);

        // then
        verify(eventConsumerApple).accept(event(someAppleEvent.getEventBody().getKey(), new Apple(), someAppleEvent.getSequenceNumber(), someAppleEvent.getArrivalTimestamp(), someAppleEvent.getDurationBehind().get()));
        verify(eventConsumerBanana).accept(event(someBananaEvent.getEventBody().getKey(), new Banana(), someBananaEvent.getSequenceNumber(), someBananaEvent.getArrivalTimestamp(), someBananaEvent.getDurationBehind().get()));
        verify(eventConsumerCherry, never()).accept(any(Event.class));
    }

    static class Apple {
        public boolean equals(Object o) {
            return o instanceof Apple;
        }
    }
    static class Banana {
        public boolean equals(Object o) {
            return o instanceof Banana;
        }
    }
    static class Cherry {
        public boolean equals(Object o) {
            return o instanceof Cherry;
        }
    }
}
