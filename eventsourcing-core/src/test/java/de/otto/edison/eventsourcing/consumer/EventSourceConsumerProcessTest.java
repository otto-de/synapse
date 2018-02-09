package de.otto.edison.eventsourcing.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.eventsourcing.event.Message;
import org.junit.Test;
import org.springframework.context.ApplicationEventPublisher;

import java.time.Duration;
import java.time.Instant;
import java.util.function.Predicate;

import static de.otto.edison.eventsourcing.consumer.TestEventConsumer.testEventConsumer;
import static de.otto.edison.eventsourcing.event.Message.message;
import static java.util.Collections.singletonList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class EventSourceConsumerProcessTest {

    private static final String TEST_STREAM_NAME = "test-stream";

    @Test
    @SuppressWarnings("unchecked")
    public void shouldInvokeTwoConsumersForSameEventSource() {
        EventSource eventSource = spy(new TestEventSource());
        TestEventConsumer eventConsumerA = spy(testEventConsumer(".*", MyPayload.class));
        TestEventConsumer eventConsumerB = spy(testEventConsumer(".*", MyPayload.class));
        eventSource.register(eventConsumerA);
        eventSource.register(eventConsumerB);

        EventSourceConsumerProcess process = new EventSourceConsumerProcess(singletonList(eventSource));
        process.start();

        verify(eventSource, timeout(1000)).consumeAll(any(StreamPosition.class), any(Predicate.class));
        verify(eventConsumerA, timeout(1000)).accept(any());
        verify(eventConsumerB, timeout(1000)).accept(any());
    }

    static class MyPayload {
        // dummy class for tests
    }

    class TestEventSource extends AbstractEventSource {

        public TestEventSource() {
            super("testEventSource", mock(ApplicationEventPublisher.class),  new ObjectMapper());
        }

        @Override
        public String getStreamName() {
            return TEST_STREAM_NAME;
        }

        @Override
        public StreamPosition consumeAll(StreamPosition startFrom, Predicate<Message<?>> stopCondition) {
            registeredConsumers().encodeAndSend(Message.message("someKey", "{}", "0", Instant.now(), Duration.ZERO));
            return StreamPosition.of();
        }
    }

}
