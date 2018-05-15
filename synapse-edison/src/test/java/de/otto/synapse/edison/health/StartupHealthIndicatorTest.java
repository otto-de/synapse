package de.otto.synapse.edison.health;

import de.otto.synapse.eventsource.EventSource;
import de.otto.synapse.eventsource.EventSourceNotification;
import org.junit.Test;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StartupHealthIndicatorTest {

    @Test
    public void shouldInitiallyIndicateDown() {
        // given
        StartupHealthIndicator healthCheck = new StartupHealthIndicator();

        // when
        Health health = healthCheck.health();

        // then
        assertThat(health.getStatus(), is(Status.DOWN));
    }

    @Test
    public void shouldIndicateUpWhenFirstEventSourceIsFinished() {
        // given
        StartupHealthIndicator healthCheck = new StartupHealthIndicator();

        EventSource mockEventSource = mock(EventSource.class);
        when(mockEventSource.getChannelName()).thenReturn("some-stream");

        EventSourceNotification eventSourceNotification = EventSourceNotification.builder()
                .withStatus(EventSourceNotification.Status.FINISHED)
                .withMessage("some message")
                .withChannelName("some-stream")
                .build();
        healthCheck.onEventSourceNotification(eventSourceNotification);

        // when
        Health health = healthCheck.health();

        // then
        assertThat(health.getStatus(), is(Status.UP));
        assertThat(health.getDetails(), hasEntry("message", "some message"));
        assertThat(health.getDetails(), hasEntry("stream", "some-stream"));
    }
}