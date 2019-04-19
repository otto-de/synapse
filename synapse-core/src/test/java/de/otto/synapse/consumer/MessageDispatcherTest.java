package de.otto.synapse.consumer;

import de.otto.synapse.message.Message;
import de.otto.synapse.message.TextMessage;
import org.junit.Test;

import static de.otto.synapse.channel.ShardPosition.fromHorizon;
import static de.otto.synapse.consumer.TestMessageConsumer.testEventConsumer;
import static de.otto.synapse.message.Header.of;
import static de.otto.synapse.message.Message.message;
import static java.util.Arrays.asList;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class MessageDispatcherTest {

    @Test
    @SuppressWarnings("unchecked")
    public void shouldDelegateEventsToAllConsumers() {
        // given
        TestMessageConsumer<Object> eventConsumerA = spy(testEventConsumer(".*", Object.class));
        TestMessageConsumer<Object> eventConsumerB = spy(testEventConsumer(".*", Object.class));
        TestMessageConsumer<Object> eventConsumerC = spy(testEventConsumer(".*", Object.class));

        MessageDispatcher messageDispatcher = new MessageDispatcher();
        messageDispatcher.add(eventConsumerA);
        messageDispatcher.add(eventConsumerB);
        messageDispatcher.add(eventConsumerC);

        // when
        TextMessage someMessage = TextMessage.of(
                "someKey",
                of(fromHorizon("test")),
                "{}"
        );
        messageDispatcher.accept(someMessage);

        // then
        verify(eventConsumerA).accept(any(Message.class));
        verify(eventConsumerB).accept(any(Message.class));
        verify(eventConsumerC).accept(any(Message.class));
    }

    @Test(expected = IllegalStateException.class)
    @SuppressWarnings("unchecked")
    public void shouldNotDelegateBrokenEventsToConsumers() {
        // given
        TestMessageConsumer<Apple> appleConsumer = spy(testEventConsumer(".*", Apple.class));
        TestMessageConsumer<Banana> bananaConsumer = spy(testEventConsumer(".*", Banana.class));

        MessageDispatcher messageDispatcher = new MessageDispatcher();
        messageDispatcher.add(appleConsumer);
        messageDispatcher.add(bananaConsumer);

        // when
        try {

            TextMessage someMessage = TextMessage.of(
                    "someKey",
                    of(fromHorizon("test")),
                    "kawumm!"
            );
            messageDispatcher.accept(someMessage);
            fail();
        } catch (final IllegalStateException e) {
            // then
            verify(appleConsumer).keyPattern();
            verify(appleConsumer).payloadType();
            verifyNoMoreInteractions(appleConsumer);
            verifyZeroInteractions(bananaConsumer);
            throw e;
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldDelegateEventsToSpecificConsumersForEventKey() {
        // given

        TestMessageConsumer<Apple> eventConsumerApple = spy(testEventConsumer("apple.*", Apple.class));
        TestMessageConsumer<Banana> eventConsumerBanana = spy(testEventConsumer("banana.*", Banana.class));
        TestMessageConsumer<Cherry> eventConsumerCherry = spy(testEventConsumer("cherry.*", Cherry.class));

        MessageDispatcher messageDispatcher = new MessageDispatcher(asList(eventConsumerApple, eventConsumerBanana, eventConsumerCherry));

        // when
        TextMessage someAppleMessage = TextMessage.of("apple.123", of(fromHorizon("test")),"{}");
        TextMessage someBananaMessage = TextMessage.of("banana.456", of(fromHorizon("test")), "{}");
        messageDispatcher.accept(someAppleMessage);
        messageDispatcher.accept(someBananaMessage);

        // then
        verify(eventConsumerApple).accept(
                message(
                        someAppleMessage.getKey(),
                        of(fromHorizon("test")),
                        new Apple()));
        verify(eventConsumerBanana).accept(
                message(
                        someBananaMessage.getKey(),
                        of(fromHorizon("test")),
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
