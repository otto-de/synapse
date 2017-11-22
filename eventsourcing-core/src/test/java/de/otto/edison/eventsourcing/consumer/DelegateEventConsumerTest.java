package de.otto.edison.eventsourcing.consumer;

import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;

import static java.util.Arrays.asList;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

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