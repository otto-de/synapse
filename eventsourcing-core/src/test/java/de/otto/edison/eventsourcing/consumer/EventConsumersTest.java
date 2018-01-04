package de.otto.edison.eventsourcing.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.Duration;
import java.time.Instant;

import static de.otto.edison.eventsourcing.consumer.TestEventConsumer.testEventConsumer;
import static java.util.Arrays.asList;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class EventConsumersTest {

    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    public static final String TEST_STREAM = "test-stream";

    @Test
    public void shouldDelegateEventsToAllConsumers() throws Exception {
        // given
        TestEventConsumer<Object> eventConsumerA = spy(testEventConsumer(TEST_STREAM, Object.class));
        TestEventConsumer<Object> eventConsumerB = spy(testEventConsumer(TEST_STREAM, Object.class));
        TestEventConsumer<Object> eventConsumerC = spy(testEventConsumer(TEST_STREAM, Object.class));

        EventConsumers eventConsumers = new EventConsumers(OBJECT_MAPPER);
        eventConsumers.add(eventConsumerA);
        eventConsumers.add(eventConsumerB);
        eventConsumers.add(eventConsumerC);

        // when
        Event<String> someEvent = new Event<>("someKey", "{}", "0", Instant.now(), Duration.ZERO);
        eventConsumers.encodeAndSend(someEvent);

        // then
        verify(eventConsumerA).accept(any(Event.class));
        verify(eventConsumerB).accept(any(Event.class));
        verify(eventConsumerC).accept(any(Event.class));
    }

    @Test
    public void shouldDelegateEventsToSpecificConsumersForEventKey() throws Exception {
        // given

        TestEventConsumer<Apple> eventConsumerApple = spy(testEventConsumer(TEST_STREAM, "apple.*", Apple.class));
        TestEventConsumer<Banana> eventConsumerBanana = spy(testEventConsumer(TEST_STREAM, "banana.*", Banana.class));
        TestEventConsumer<Cherry> eventConsumerCherry = spy(testEventConsumer(TEST_STREAM, "cherry.*", Cherry.class));

        EventConsumers eventConsumers = new EventConsumers(OBJECT_MAPPER, asList(eventConsumerApple, eventConsumerBanana, eventConsumerCherry));

        // when
        Event<String> someAppleEvent = new Event<>("apple.123", "{}", "0", Instant.now(), Duration.ZERO);
        Event<String> someBananaEvent = new Event<>("banana.456", "{}", "0", Instant.now(), Duration.ZERO);
        eventConsumers.encodeAndSend(someAppleEvent);
        eventConsumers.encodeAndSend(someBananaEvent);

        // then
        verify(eventConsumerApple).accept(new Event<>(someAppleEvent.key(), new Apple(), someAppleEvent.sequenceNumber(), someAppleEvent.arrivalTimestamp(), someAppleEvent.durationBehind().get()));
        verify(eventConsumerBanana).accept(new Event<>(someBananaEvent.key(), new Banana(), someBananaEvent.sequenceNumber(), someBananaEvent.arrivalTimestamp(), someBananaEvent.durationBehind().get()));
        verify(eventConsumerCherry, never()).accept(any(Event.class));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionWhenConsumersHaveDifferentStreamNames() {
        // given
        EventSource eventSourceMock = Mockito.mock(EventSource.class);
        when(eventSourceMock.getStreamName()).thenReturn("test-stream-A");

        TestEventConsumer<Object> eventConsumerA = spy(testEventConsumer("test-stream-A", Object.class));
        TestEventConsumer<Object> eventConsumerB = spy(testEventConsumer("test-stream-B", Object.class));


        // when
        new EventConsumers(OBJECT_MAPPER, asList(eventConsumerA, eventConsumerB));

        // then expect exception
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