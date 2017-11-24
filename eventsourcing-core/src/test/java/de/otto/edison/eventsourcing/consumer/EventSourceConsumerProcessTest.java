package de.otto.edison.eventsourcing.consumer;

import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static java.util.Arrays.asList;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class EventSourceConsumerProcessTest {

    private static final String TEST_STREAM_NAME = "test-stream";

    @Test
    public void shouldInvokeTwoConsumersForSameEventSource() throws Exception {
        EventSource<MyPayload> eventSource = spy(new TestEventSource());
        TestEventConsumer eventConsumerA = spy(new TestEventConsumer());
        TestEventConsumer eventConsumerB = spy(new TestEventConsumer());

        EventSourceConsumerProcess process = new EventSourceConsumerProcess(
                asList(eventSource),
                asList(eventConsumerA, eventConsumerB));

        process.init();
        Thread.sleep(100L);

        verify(eventSource).consumeAll(any(StreamPosition.class), any(Predicate.class), any(Consumer.class));
        verify(eventConsumerA).accept(any());
        verify(eventConsumerB).accept(any());
    }

    class MyPayload {
        // dummy class for tests
    }

    class TestEventSource implements EventSource<MyPayload> {

        @Override
        public String getStreamName() {
            return TEST_STREAM_NAME;
        }

        @Override
        public StreamPosition consumeAll(StreamPosition startFrom, Predicate<Event<MyPayload>> stopCondition, Consumer<Event<MyPayload>> consumer) {
            consumer.accept(new Event<>("someKey", new MyPayload(), "0", Instant.now(), Duration.ZERO));
            return new StreamPosition(Collections.emptyMap());
        }
    }

}