package de.otto.edison.eventsourcing.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.function.Predicate;

import static de.otto.edison.eventsourcing.consumer.TestEventConsumer.testEventConsumer;
import static java.util.Collections.singletonList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class EventSourceConsumerProcessTest {

    private static final String TEST_STREAM_NAME = "test-stream";

    @Test
    @SuppressWarnings("unchecked")
    public void shouldInvokeTwoConsumersForSameEventSource() throws Exception {
        EventSource eventSource = spy(new TestEventSource());
        TestEventConsumer eventConsumerA = spy(testEventConsumer(".*", MyPayload.class));
        TestEventConsumer eventConsumerB = spy(testEventConsumer(".*", MyPayload.class));
        eventSource.register(eventConsumerA);
        eventSource.register(eventConsumerB);

        EventSourceConsumerProcess process = new EventSourceConsumerProcess(singletonList(eventSource));
        process.start();
        Thread.sleep(100L);

        verify(eventSource).consumeAll(any(StreamPosition.class), any(Predicate.class));
        verify(eventConsumerA).accept(any());
        verify(eventConsumerB).accept(any());
    }

    static class MyPayload {
        // dummy class for tests
    }

    class TestEventSource extends AbstractEventSource {

        public TestEventSource() {
            super("testEventSource", new ObjectMapper());
        }

        @Override
        public String getStreamName() {
            return TEST_STREAM_NAME;
        }

        @Override
        public StreamPosition consumeAll(StreamPosition startFrom, Predicate<Event<?>> stopCondition) {
            registeredConsumers().encodeAndSend(new Event<>("someKey", "{}", "0", Instant.now(), Duration.ZERO));
            return StreamPosition.of();
        }
    }

}
