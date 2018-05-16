package de.otto.synapse.edison.health;

import com.google.common.collect.ImmutableMap;
import de.otto.synapse.eventsource.EventSource;
import de.otto.synapse.eventsource.EventSourceNotification;
import org.junit.Test;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import static de.otto.synapse.eventsource.EventSourceNotification.builder;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StartupHealthIndicatorTest {

    @Test
    public void shouldInitiallyIndicateDownIfNotConfigured() {
        // given
        HealthProperties properties = new HealthProperties();
        SnapshotStatusProvider provider = new SnapshotStatusProvider(properties);
        StartupHealthIndicator healthCheck = new StartupHealthIndicator(provider);

        // when
        Health health = healthCheck.health();

        // then
        assertThat(health.getStatus(), is(Status.UP));
    }

    @Test
    public void shouldInitiallyIndicateDownIfChannelsConfigured() {
        // given
        HealthProperties properties = new HealthProperties();
        properties.setChannels(asList("some-stream", "other-stream"));
        SnapshotStatusProvider provider = new SnapshotStatusProvider(properties);

        StartupHealthIndicator healthCheck = new StartupHealthIndicator(provider);

        // when
        Health health = healthCheck.health();

        // then
        assertThat(health.getStatus(), is(Status.DOWN));
    }

    @Test
    public void shouldIndicateDownBeforeLastChannelHasFinished() {
        // given
        HealthProperties properties = new HealthProperties();
        properties.setChannels(asList("some-stream", "other-stream"));
        SnapshotStatusProvider provider = new SnapshotStatusProvider(properties);
        StartupHealthIndicator healthCheck = new StartupHealthIndicator(provider);

        EventSource mockEventSource = mock(EventSource.class);
        when(mockEventSource.getChannelName()).thenReturn("some-stream");

        // when
        healthCheck.onEventSourceNotification(builder()
                .withStatus(EventSourceNotification.Status.FINISHED)
                .withMessage("Finished some-stream")
                .withChannelName("some-stream")
                .build());

        // then
        Health health = healthCheck.health();
        assertThat(health.getStatus(), is(Status.DOWN));
        assertThat(health.getDetails(), hasEntry("some-stream", ImmutableMap.of("message", "Finished some-stream", "status", "FINISHED")));
        assertThat(health.getDetails(), hasEntry("other-stream", ImmutableMap.of("message", "Channel not yet finished", "status", "NOT FINISHED")));

    }

    @Test
    public void shouldIndicateUpAfterLastChannelHasFinished() {
        // given
        HealthProperties properties = new HealthProperties();
        properties.setChannels(asList("some-stream", "other-stream"));
        SnapshotStatusProvider provider = new SnapshotStatusProvider(properties);
        StartupHealthIndicator healthCheck = new StartupHealthIndicator(provider);

        EventSource mockEventSource = mock(EventSource.class);
        when(mockEventSource.getChannelName()).thenReturn("some-stream");

        // when
        healthCheck.onEventSourceNotification(builder()
                .withStatus(EventSourceNotification.Status.FINISHED)
                .withMessage("Finished some-stream")
                .withChannelName("some-stream")
                .build());

        healthCheck.onEventSourceNotification(builder()
                .withStatus(EventSourceNotification.Status.FINISHED)
                .withMessage("Finished other-stream")
                .withChannelName("other-stream")
                .build());

        // then
        Health health = healthCheck.health();
        assertThat(health.getStatus(), is(Status.UP));
        assertThat(health.getDetails(), hasEntry("some-stream", ImmutableMap.of("message", "Finished some-stream", "status", "FINISHED")));
        assertThat(health.getDetails(), hasEntry("other-stream", ImmutableMap.of("message", "Finished other-stream", "status", "FINISHED")));
    }
}
