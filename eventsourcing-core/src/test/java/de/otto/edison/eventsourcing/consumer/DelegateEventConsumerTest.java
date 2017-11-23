package de.otto.edison.eventsourcing.consumer;

import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;

import static java.util.Arrays.asList;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class DelegateEventConsumerTest {

    @Test
    public void shouldDelegateEventsToAllConsumers() throws Exception {
        // given
        TestEventConsumer eventConsumerA = spy(new TestEventConsumer());
        TestEventConsumer eventConsumerB = spy(new TestEventConsumer());
        TestEventConsumer eventConsumerC = spy(new TestEventConsumer());
        DelegateEventConsumer<Object> delegateConsumer = spy(new DelegateEventConsumer<>(
                asList(eventConsumerA, eventConsumerB, eventConsumerC)));

        // when
        Event<Object> someEvent = new Event<>("someKey", new Object(), "0", Instant.now(), Duration.ZERO);
        delegateConsumer.consumerFunction().accept(someEvent);

        // then
        verify(eventConsumerA).accept(any(Event.class));
        verify(eventConsumerB).accept(any(Event.class));
        verify(eventConsumerC).accept(any(Event.class));
    }

    @Test
    public void shouldDelegateEventsToSpecificConsumersForEventKey() throws Exception {
        // given
        TestEventConsumer eventConsumerA = spy(new TestEventConsumer().setKeyPattern("apple.*"));
        TestEventConsumer eventConsumerB = spy(new TestEventConsumer().setKeyPattern("banana.*"));
        TestEventConsumer eventConsumerC = spy(new TestEventConsumer().setKeyPattern("orange.*"));
        DelegateEventConsumer<Object> delegateConsumer = spy(new DelegateEventConsumer<>(
                asList(eventConsumerA, eventConsumerB, eventConsumerC)));

        // when
        Event<Object> someEventForA = new Event<>("apple.123", new Object(), "0", Instant.now(), Duration.ZERO);
        Event<Object> someEventForB = new Event<>("banana.456", new Object(), "0", Instant.now(), Duration.ZERO);
       delegateConsumer.consumerFunction().accept(someEventForA);
       delegateConsumer.consumerFunction().accept(someEventForB);

        // then
        verify(eventConsumerA).accept(someEventForA);
        verify(eventConsumerB).accept(someEventForB);
        verify(eventConsumerC, never()).accept(any(Event.class));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionWhenListOfConsumersIsEmpty() {
        new DelegateEventConsumer<>(Collections.emptyList());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionWhenConsumersHaveDifferentStreamNames() {
        new DelegateEventConsumer<>(asList(
            new TestEventConsumer<>().setStreamName("streamNameA"),
            new TestEventConsumer<>().setStreamName("streamNameB")
        ));
    }

}