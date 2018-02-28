package de.otto.synapse.eventsource;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.consumer.TestMessageConsumer;
import de.otto.synapse.message.Message;
import org.junit.Test;
import org.springframework.context.ApplicationEventPublisher;

import java.time.Duration;
import java.time.Instant;
import java.util.function.Predicate;

import static de.otto.synapse.consumer.TestMessageConsumer.testEventConsumer;
import static de.otto.synapse.message.Header.responseHeader;
import static de.otto.synapse.message.Message.message;
import static java.util.Collections.singletonList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

public class EventSourceConsumerProcessTest {

    private static final String TEST_STREAM_NAME = "test-stream";

    @Test
    @SuppressWarnings("unchecked")
    public void shouldInvokeTwoConsumersForSameEventSource() {
        EventSource eventSource = spy(new TestEventSource());
        TestMessageConsumer eventConsumerA = spy(testEventConsumer(".*", MyPayload.class));
        TestMessageConsumer eventConsumerB = spy(testEventConsumer(".*", MyPayload.class));
        eventSource.register(eventConsumerA);
        eventSource.register(eventConsumerB);

        EventSourceConsumerProcess process = new EventSourceConsumerProcess(singletonList(eventSource));
        process.start();

        verify(eventSource, timeout(1000)).consumeAll(any(ChannelPosition.class), any(Predicate.class));
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
        public ChannelPosition consumeAll(ChannelPosition startFrom, Predicate<Message<?>> stopCondition) {
            final Message<String> message = message(
                    "someKey",
                    responseHeader(null, Instant.now(), Duration.ZERO),
                    "{}"
            );
            dispatchingMessageConsumer().accept(message);
            return ChannelPosition.fromHorizon();
        }
    }

}
