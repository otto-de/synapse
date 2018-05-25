package de.otto.synapse.eventsource;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.channel.InMemoryChannel;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.message.Message;
import org.junit.Test;
import org.springframework.context.ApplicationEventPublisher;

import javax.annotation.Nonnull;
import java.util.regex.Pattern;

import static de.otto.synapse.message.Message.message;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class InMemoryEventSourceTest {

    private final ApplicationEventPublisher eventPublisher = mock(ApplicationEventPublisher.class);
    private ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void shouldSendEventInStreamToConsumer() {
        // given
        InMemoryChannel inMemoryChannel = new InMemoryChannel("some-stream", objectMapper, eventPublisher);
        InMemoryEventSource inMemoryEventSource = new InMemoryEventSource("es", inMemoryChannel);
        StringMessageConsumer eventConsumer = new StringMessageConsumer();
        inMemoryEventSource.register(eventConsumer);
        inMemoryChannel.send(message("key", "payload"));
        inMemoryEventSource.stop();

        // when
        inMemoryEventSource.consume();


        // then
        assertThat(eventConsumer.message.getKey(), is("key"));
        assertThat(eventConsumer.message.getPayload(), is("payload"));
    }
    
    private static class StringMessageConsumer implements MessageConsumer<String> {
        private Message<String> message;

        @Nonnull
        @Override
        public Class<String> payloadType() {
            return String.class;
        }

        @Nonnull
        @Override
        public Pattern keyPattern() {
            return Pattern.compile(".*");
        }

        @Override
        public void accept(Message<String> message) {
            this.message = message;
        }
    }
}
