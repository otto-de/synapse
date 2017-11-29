package de.otto.edison.eventsourcing.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.eventsourcing.annotation.EventSourceMapping;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.Duration;
import java.time.Instant;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class DelegateEventConsumerTest {

    @Test
    public void shouldDelegateEventsToAllConsumers() throws Exception {
        // given
        String streamName = "test-stream";
        EventSource<String> eventSourceMock = Mockito.mock(EventSource.class);
        when(eventSourceMock.getStreamName()).thenReturn(streamName);

        TestEventConsumer eventConsumerA = spy(new TestEventConsumer().setStreamName(streamName));
        TestEventConsumer eventConsumerB = spy(new TestEventConsumer().setStreamName(streamName));
        TestEventConsumer eventConsumerC = spy(new TestEventConsumer().setStreamName(streamName));

        EventSourceMapping.ConsumerMapping consumerMapping = new EventSourceMapping().getConsumerMapping(eventSourceMock);
        consumerMapping.addConsumerAndPayloadForKeyPattern(".*", eventConsumerA, Object.class);
        consumerMapping.addConsumerAndPayloadForKeyPattern(".*", eventConsumerB, Object.class);
        consumerMapping.addConsumerAndPayloadForKeyPattern(".*", eventConsumerC, Object.class);

        DelegateEventConsumer delegateConsumer = spy(new DelegateEventConsumer(consumerMapping, new ObjectMapper()));

        // when
        Event<String> someEvent = new Event<>("someKey", "{}", "0", Instant.now(), Duration.ZERO);
        delegateConsumer.consumerFunction().accept(someEvent);

        // then
        verify(eventConsumerA).accept(any(Event.class));
        verify(eventConsumerB).accept(any(Event.class));
        verify(eventConsumerC).accept(any(Event.class));
    }

    @Test
    public void shouldDelegateEventsToSpecificConsumersForEventKey() throws Exception {
        // given
        String streamName = "test-stream";
        EventSource<String> eventSourceMock = Mockito.mock(EventSource.class);
        when(eventSourceMock.getStreamName()).thenReturn(streamName);

        TestEventConsumer eventConsumerApple = spy(new TestEventConsumer().setStreamName(streamName));
        TestEventConsumer eventConsumerBanana = spy(new TestEventConsumer().setStreamName(streamName));
        TestEventConsumer eventConsumerCherry = spy(new TestEventConsumer().setStreamName(streamName));

        EventSourceMapping.ConsumerMapping consumerMapping = new EventSourceMapping().getConsumerMapping(eventSourceMock);
        consumerMapping.addConsumerAndPayloadForKeyPattern("apple.*", eventConsumerApple, Apple.class);
        consumerMapping.addConsumerAndPayloadForKeyPattern("banana.*", eventConsumerBanana, Banana.class);
        consumerMapping.addConsumerAndPayloadForKeyPattern("cherry.*", eventConsumerCherry, Cherry.class);

        DelegateEventConsumer delegateConsumer = spy(new DelegateEventConsumer(consumerMapping, new ObjectMapper()));

        // when
        Event<String> someEventForA = new Event<>("apple.123", "{}", "0", Instant.now(), Duration.ZERO);
        Event<String> someEventForB = new Event<>("banana.456", "{}", "0", Instant.now(), Duration.ZERO);
        delegateConsumer.consumerFunction().accept(someEventForA);
        delegateConsumer.consumerFunction().accept(someEventForB);

        // then
        verify(eventConsumerApple).accept(new Event(someEventForA.key(), new Apple(), someEventForA.sequenceNumber(), someEventForA.arrivalTimestamp(), someEventForA.durationBehind().get()));
        verify(eventConsumerBanana).accept(new Event(someEventForB.key(), new Banana(), someEventForB.sequenceNumber(), someEventForB.arrivalTimestamp(), someEventForB.durationBehind().get()));
        verify(eventConsumerCherry, never()).accept(any(Event.class));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionWhenListOfConsumersIsEmpty() {
        // given
        String streamName = "test-stream";
        EventSource<String> eventSourceMock = Mockito.mock(EventSource.class);
        when(eventSourceMock.getStreamName()).thenReturn(streamName);

        // when
        new DelegateEventConsumer(new EventSourceMapping().getConsumerMapping(eventSourceMock), new ObjectMapper());

        // then expect exception
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionWhenConsumersHaveDifferentStreamNames() {
        // given
        EventSource<String> eventSourceMock = Mockito.mock(EventSource.class);
        when(eventSourceMock.getStreamName()).thenReturn("test-stream-A");

        TestEventConsumer eventConsumerA = spy(new TestEventConsumer().setStreamName("test-stream-A"));
        TestEventConsumer eventConsumerB = spy(new TestEventConsumer().setStreamName("test-stream-B"));

        EventSourceMapping.ConsumerMapping consumerMapping = new EventSourceMapping().getConsumerMapping(eventSourceMock);
        consumerMapping.addConsumerAndPayloadForKeyPattern(".*", eventConsumerA, Object.class);
        consumerMapping.addConsumerAndPayloadForKeyPattern(".*", eventConsumerB, Object.class);

        // when
        new DelegateEventConsumer(consumerMapping, new ObjectMapper());

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