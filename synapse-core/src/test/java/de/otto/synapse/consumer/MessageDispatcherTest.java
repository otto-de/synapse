package de.otto.synapse.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.message.Message;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;

import static de.otto.synapse.channel.ChannelPosition.fromHorizon;
import static de.otto.synapse.consumer.TestMessageConsumer.testEventConsumer;
import static de.otto.synapse.message.Header.responseHeader;
import static de.otto.synapse.message.Message.message;
import static java.util.Arrays.asList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class MessageDispatcherTest {

    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    @SuppressWarnings("unchecked")
    public void shouldDelegateEventsToAllConsumers() {
        // given
        TestMessageConsumer<Object> eventConsumerA = spy(testEventConsumer(".*", Object.class));
        TestMessageConsumer<Object> eventConsumerB = spy(testEventConsumer(".*", Object.class));
        TestMessageConsumer<Object> eventConsumerC = spy(testEventConsumer(".*", Object.class));

        MessageDispatcher messageDispatcher = new MessageDispatcher(OBJECT_MAPPER);
        messageDispatcher.add(eventConsumerA);
        messageDispatcher.add(eventConsumerB);
        messageDispatcher.add(eventConsumerC);

        // when
        Message<String> someMessage = message(
                "someKey",
                responseHeader(fromHorizon(), Instant.now(), Duration.ZERO),
                "{}"
        );
        messageDispatcher.accept(someMessage);

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

        MessageDispatcher messageDispatcher = new MessageDispatcher(OBJECT_MAPPER, asList(eventConsumerApple, eventConsumerBanana, eventConsumerCherry));

        // when
        final Instant now = Instant.now();
        Message<String> someAppleMessage = message("apple.123", responseHeader(fromHorizon(), now, Duration.ZERO),"{}");
        Message<String> someBananaMessage = message("banana.456", responseHeader(fromHorizon(), now, Duration.ZERO), "{}");
        messageDispatcher.accept(someAppleMessage);
        messageDispatcher.accept(someBananaMessage);

        // then
        verify(eventConsumerApple).accept(
                message(
                        someAppleMessage.getKey(),
                        responseHeader(fromHorizon(), now, Duration.ZERO),
                        new Apple()));
        verify(eventConsumerBanana).accept(
                message(
                        someBananaMessage.getKey(),
                        responseHeader(fromHorizon(), now, Duration.ZERO),
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
