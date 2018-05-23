package de.otto.synapse.eventsource;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.InMemoryChannel;
import de.otto.synapse.channel.StartFrom;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.info.MessageEndpointNotification;
import de.otto.synapse.info.MessageEndpointStatus;
import de.otto.synapse.message.Message;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.context.ApplicationEventPublisher;

import javax.annotation.Nonnull;
import java.util.regex.Pattern;

import static de.otto.synapse.message.Message.message;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

public class InMemoryEventSourceTest {

    private final ApplicationEventPublisher eventPublisher = mock(ApplicationEventPublisher.class);
    private ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void shouldSendEventInStreamToConsumer() {
        // given
        InMemoryChannel inMemoryChannel = new InMemoryChannel("some-stream", objectMapper, eventPublisher);
        InMemoryEventSource inMemoryEventSource = new InMemoryEventSource("es", inMemoryChannel, eventPublisher);
        StringMessageConsumer eventConsumer = new StringMessageConsumer();
        inMemoryEventSource.register(eventConsumer);
        inMemoryChannel.send(message("key", "payload"));
        inMemoryEventSource.stop();

        // when
        inMemoryEventSource.consume(event -> true);


        // then
        assertThat(eventConsumer.message.getKey(), is("key"));
        assertThat(eventConsumer.message.getPayload(), is("payload"));
    }

    @Test
    public void shouldPublishStartedAndFinishedEvents() {
        // given
        InMemoryChannel inMemoryChannel = new InMemoryChannel("some-stream", objectMapper, eventPublisher);
        InMemoryEventSource inMemoryEventSource = new InMemoryEventSource("es", inMemoryChannel, eventPublisher);
        StringMessageConsumer eventConsumer = new StringMessageConsumer();
        inMemoryEventSource.register(eventConsumer);
        inMemoryChannel.send(message("key", "payload"));
        inMemoryEventSource.stop();

        // when
        inMemoryEventSource.consume(event -> true);


        // then
        ArgumentCaptor<MessageEndpointNotification> notificationArgumentCaptor = ArgumentCaptor.forClass(MessageEndpointNotification.class);
        verify(eventPublisher, times(2)).publishEvent(notificationArgumentCaptor.capture());

        MessageEndpointNotification startedEvent = notificationArgumentCaptor.getAllValues().get(0);
        assertThat(startedEvent.getStatus(), is(MessageEndpointStatus.STARTING));
        assertThat(startedEvent.getChannelPosition(), is(ChannelPosition.fromHorizon()));
        assertThat(startedEvent.getChannelName(), is("some-stream"));

        MessageEndpointNotification finishedEvent = notificationArgumentCaptor.getAllValues().get(1);
        assertThat(finishedEvent.getStatus(), is(MessageEndpointStatus.FINISHED));
        assertThat(finishedEvent.getChannelPosition().shard("some-stream").startFrom(), is(StartFrom.POSITION));
        assertThat(finishedEvent.getChannelPosition().shard("some-stream").position(), is("0"));
        assertThat(finishedEvent.getChannelName(), is("some-stream"));
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
