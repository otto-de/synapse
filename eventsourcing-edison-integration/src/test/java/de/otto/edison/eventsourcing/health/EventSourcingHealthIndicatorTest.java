package de.otto.edison.eventsourcing.health;

import de.otto.edison.eventsourcing.consumer.EventSource;
import de.otto.edison.eventsourcing.consumer.EventSourceNotification;
import org.junit.Test;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EventSourcingHealthIndicatorTest {

    @Test
    public void shouldIndicateUpNormally() {
        // given
        EventSourcingHealthIndicator healthCheck = new EventSourcingHealthIndicator();

        // when
        Health health = healthCheck.health();

        // then
        assertThat(health.getStatus(), is(Status.UP));
    }

    @Test
    public void shouldIndicateDownWhenSomeEventSourceIsFailed() {
        // given
        EventSourcingHealthIndicator healthCheck = new EventSourcingHealthIndicator();

        EventSource mockEventSource = mock(EventSource.class);
        when(mockEventSource.getStreamName()).thenReturn("some-stream");

        EventSourceNotification eventSourceNotification = EventSourceNotification.builder()
                .withStatus(EventSourceNotification.Status.FAILED)
                .withMessage("some message")
                .withStreamName("some-stream")
                .build();
        healthCheck.onEventSourceNotification(eventSourceNotification);

        // when
        Health health = healthCheck.health();

        // then
        assertThat(health.getStatus(), is(Status.DOWN));
        assertThat(health.getDetails(), hasEntry("message", "some message"));
        assertThat(health.getDetails(), hasEntry("stream", "some-stream"));
    }
}