package de.otto.edison.eventsourcing.consumer;

import de.otto.edison.eventsourcing.annotation.EventSourceMapping;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.function.Predicate;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class EventSourceConsumerProcessTest {

    private static final String TEST_STREAM_NAME = "test-stream";

    @Test
    public void shouldInvokeTwoConsumersForSameEventSource() throws Exception {
        EventSource<String> eventSource = spy(new TestEventSource());
        TestEventConsumer eventConsumerA = spy(new TestEventConsumer());
        TestEventConsumer eventConsumerB = spy(new TestEventConsumer());

        EventSourceMapping eventSourceMapping = new EventSourceMapping();
        eventSourceMapping.getConsumerMapping(eventSource).addConsumerAndPayloadForKeyPattern(".*", eventConsumerA, MyPayload.class);
        eventSourceMapping.getConsumerMapping(eventSource).addConsumerAndPayloadForKeyPattern(".*", eventConsumerB, MyPayload.class);

        EventSourceConsumerProcess process = new EventSourceConsumerProcess(eventSourceMapping);

        process.init();
        Thread.sleep(100L);

        verify(eventSource).consumeAll(any(StreamPosition.class), any(Predicate.class), any(EventConsumer.class));
        verify(eventConsumerA).accept(any());
        verify(eventConsumerB).accept(any());
    }

    static class MyPayload {
        // dummy class for tests
    }

    class TestEventSource implements EventSource<String> {

        @Override
        public String getStreamName() {
            return TEST_STREAM_NAME;
        }

        @Override
        public StreamPosition consumeAll(StreamPosition startFrom, Predicate<Event<String>> stopCondition, EventConsumer<String> consumer) {
            consumer.accept(new Event<>("someKey", "{}", "0", Instant.now(), Duration.ZERO));
            return new StreamPosition(Collections.emptyMap());
        }
    }

}