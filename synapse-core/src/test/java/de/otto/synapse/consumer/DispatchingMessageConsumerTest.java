package de.otto.synapse.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.message.Message;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;

import static de.otto.synapse.consumer.TestMessageConsumer.testEventConsumer;
import static de.otto.synapse.message.Header.responseHeader;
import static de.otto.synapse.message.Message.message;
import static java.util.Arrays.asList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class DispatchingMessageConsumerTest {

    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    @SuppressWarnings("unchecked")
    public void shouldDelegateEventsToAllConsumers() {
        // given
        TestMessageConsumer<Object> eventConsumerA = spy(testEventConsumer(".*", Object.class));
        TestMessageConsumer<Object> eventConsumerB = spy(testEventConsumer(".*", Object.class));
        TestMessageConsumer<Object> eventConsumerC = spy(testEventConsumer(".*", Object.class));

        DispatchingMessageConsumer dispatchingMessageConsumer = new DispatchingMessageConsumer(OBJECT_MAPPER);
        dispatchingMessageConsumer.add(eventConsumerA);
        dispatchingMessageConsumer.add(eventConsumerB);
        dispatchingMessageConsumer.add(eventConsumerC);

        // when
        Message<String> someMessage = message(
                "someKey",
                responseHeader("0", Instant.now(), Duration.ZERO),
                "{}"
        );
        dispatchingMessageConsumer.accept(someMessage);

        // then
        verify(eventConsumerA).accept(any(Message.class));
        verify(eventConsumerB).accept(any(Message.class));
        verify(eventConsumerC).accept(any(Message.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldDelegateEventsToSpecificConsumersForEventKey() {
        // given

        TestMessageConsumer<Apple> eventConsumerApple = spy(testEventConsumer("apple.*", Apple.class));
        TestMessageConsumer<Banana> eventConsumerBanana = spy(testEventConsumer("banana.*", Banana.class));
        TestMessageConsumer<Cherry> eventConsumerCherry = spy(testEventConsumer("cherry.*", Cherry.class));

        DispatchingMessageConsumer dispatchingMessageConsumer = new DispatchingMessageConsumer(OBJECT_MAPPER, asList(eventConsumerApple, eventConsumerBanana, eventConsumerCherry));

        // when
        Message<String> someAppleMessage = message("apple.123", responseHeader("0", Instant.now(), Duration.ZERO),"{}");
        Message<String> someBananaMessage = message("banana.456", responseHeader("0", Instant.now(), Duration.ZERO), "{}");
        dispatchingMessageConsumer.accept(someAppleMessage);
        dispatchingMessageConsumer.accept(someBananaMessage);

        // then
        verify(eventConsumerApple).accept(
                message(
                        someAppleMessage.getKey(),
                        responseHeader(someAppleMessage.getHeader().getSequenceNumber(), someAppleMessage.getHeader().getArrivalTimestamp(), someAppleMessage.getHeader().getDurationBehind().get()),
                        new Apple()));
        verify(eventConsumerBanana).accept(
                message(
                        someBananaMessage.getKey(),
                        responseHeader(someBananaMessage.getHeader().getSequenceNumber(), someBananaMessage.getHeader().getArrivalTimestamp(), someBananaMessage.getHeader().getDurationBehind().get()),
                        new Banana()));
        verify(eventConsumerCherry, never()).accept(any(Message.class));
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
