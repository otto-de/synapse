package de.otto.synapse.channel;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.info.MessageReceiverNotification;
import de.otto.synapse.info.MessageReceiverStatus;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.context.ApplicationEventPublisher;

import java.time.Instant;

import static de.otto.synapse.channel.ChannelPosition.fromHorizon;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

public class InMemoryChannelTest {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ApplicationEventPublisher eventPublisher = mock(ApplicationEventPublisher.class);

    @Test
    public void shouldPublishStartedAndFinishedEvents() {
        // given
        InMemoryChannel inMemoryChannel = new InMemoryChannel("some-stream", objectMapper, eventPublisher);

        // when
        inMemoryChannel.consumeUntil(fromHorizon(), Instant.now());

        // then
        ArgumentCaptor<MessageReceiverNotification> notificationArgumentCaptor = ArgumentCaptor.forClass(MessageReceiverNotification.class);
        verify(eventPublisher, times(3)).publishEvent(notificationArgumentCaptor.capture());

        MessageReceiverNotification startingNotification = notificationArgumentCaptor.getAllValues().get(0);
        assertThat(startingNotification.getStatus(), is(MessageReceiverStatus.STARTING));
        assertThat(startingNotification.getChannelDurationBehind().isPresent(), is(false));
        assertThat(startingNotification.getChannelName(), is("some-stream"));

        MessageReceiverNotification startedNotification = notificationArgumentCaptor.getAllValues().get(1);
        assertThat(startedNotification.getStatus(), is(MessageReceiverStatus.STARTED));
        assertThat(startedNotification.getChannelDurationBehind().isPresent(), is(false));
        assertThat(startedNotification.getChannelName(), is("some-stream"));

        MessageReceiverNotification finishedNotification = notificationArgumentCaptor.getAllValues().get(2);
        assertThat(finishedNotification.getStatus(), is(MessageReceiverStatus.FINISHED));
        assertThat(finishedNotification.getChannelDurationBehind().isPresent(), is(false));
        assertThat(finishedNotification.getChannelName(), is("some-stream"));
    }
}