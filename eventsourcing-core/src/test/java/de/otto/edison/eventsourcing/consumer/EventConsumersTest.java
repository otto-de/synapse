package de.otto.edison.eventsourcing.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.eventsourcing.message.Message;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;

import static de.otto.edison.eventsourcing.consumer.TestEventConsumer.testEventConsumer;
import static de.otto.edison.eventsourcing.message.Header.responseHeader;
import static de.otto.edison.eventsourcing.message.Message.message;
import static java.util.Arrays.asList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class EventConsumersTest {

    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    @SuppressWarnings("unchecked")
    public void shouldDelegateEventsToAllConsumers() {
        // given
        TestEventConsumer<Object> eventConsumerA = spy(testEventConsumer(".*", Object.class));
        TestEventConsumer<Object> eventConsumerB = spy(testEventConsumer(".*", Object.class));
        TestEventConsumer<Object> eventConsumerC = spy(testEventConsumer(".*", Object.class));

        EventConsumers eventConsumers = new EventConsumers(OBJECT_MAPPER);
        eventConsumers.add(eventConsumerA);
        eventConsumers.add(eventConsumerB);
        eventConsumers.add(eventConsumerC);

        // when
        Message<String> someMessage = message("someKey", "{}", "0", Instant.now(), Duration.ZERO);
        eventConsumers.encodeAndSend(someMessage);

        // then
        verify(eventConsumerA).accept(any(Message.class));
        verify(eventConsumerB).accept(any(Message.class));
        verify(eventConsumerC).accept(any(Message.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldDelegateEventsToSpecificConsumersForEventKey() {
        // given

        TestEventConsumer<Apple> eventConsumerApple = spy(testEventConsumer("apple.*", Apple.class));
        TestEventConsumer<Banana> eventConsumerBanana = spy(testEventConsumer("banana.*", Banana.class));
        TestEventConsumer<Cherry> eventConsumerCherry = spy(testEventConsumer("cherry.*", Cherry.class));

        EventConsumers eventConsumers = new EventConsumers(OBJECT_MAPPER, asList(eventConsumerApple, eventConsumerBanana, eventConsumerCherry));

        // when
        Message<String> someAppleMessage = message("apple.123", responseHeader("0", Instant.now(), Duration.ZERO),"{}");
        Message<String> someBananaMessage = message("banana.456", responseHeader("0", Instant.now(), Duration.ZERO), "{}");
        eventConsumers.encodeAndSend(someAppleMessage);
        eventConsumers.encodeAndSend(someBananaMessage);

        // then
        verify(eventConsumerApple).accept(
                message(someAppleMessage.getKey(), responseHeader(someAppleMessage.getHeader().getSequenceNumber(), someAppleMessage.getArrivalTimestamp(), someAppleMessage.getDurationBehind().get()), new Apple()));
        verify(eventConsumerBanana).accept(
                message(someBananaMessage.getKey(), responseHeader(someBananaMessage.getHeader().getSequenceNumber(), someBananaMessage.getArrivalTimestamp(), someBananaMessage.getDurationBehind().get()), new Banana()));
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
