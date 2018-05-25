package de.otto.synapse.edison.health;

import de.otto.synapse.eventsource.EventSource;
import de.otto.synapse.info.MessageReceiverNotification;
import org.junit.Test;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import static de.otto.synapse.info.MessageReceiverNotification.builder;
import static de.otto.synapse.info.MessageReceiverStatus.FAILED;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MessageReceiverEndpointHealthIndicatorTest {

    @Test
    public void shouldIndicateUpNormally() {
        // given
        MessageReceiverEndpointHealthIndicator healthCheck = new MessageReceiverEndpointHealthIndicator();

        // when
        Health health = healthCheck.health();

        // then
        assertThat(health.getStatus(), is(Status.UP));
    }

    @Test
    public void shouldIndicateDownWhenSomeEventSourceIsFailed() {
        // given
        MessageReceiverEndpointHealthIndicator healthCheck = new MessageReceiverEndpointHealthIndicator();

        EventSource mockEventSource = mock(EventSource.class);
        when(mockEventSource.getChannelName()).thenReturn("some-stream");

        MessageReceiverNotification messageEndpointNotification = builder()
                .withStatus(FAILED)
                .withMessage("some message")
                .withChannelName("some-stream")
                .build();
        healthCheck.on(messageEndpointNotification);

        // when
        Health health = healthCheck.health();

        // then
        assertThat(health.getStatus(), is(Status.DOWN));
        assertThat(health.getDetails(), hasEntry("message", "some message"));
        assertThat(health.getDetails(), hasEntry("channelName", "some-stream"));
    }
}