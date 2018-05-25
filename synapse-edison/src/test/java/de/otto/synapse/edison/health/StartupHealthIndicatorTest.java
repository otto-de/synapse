package de.otto.synapse.edison.health;

import de.otto.synapse.channel.ChannelDurationBehind;
import de.otto.synapse.eventsource.EventSource;
import org.junit.Test;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static de.otto.synapse.channel.ChannelDurationBehind.channelDurationBehind;
import static de.otto.synapse.channel.ChannelDurationBehind.unknown;
import static de.otto.synapse.info.MessageReceiverNotification.builder;
import static de.otto.synapse.info.MessageReceiverStatus.FAILED;
import static de.otto.synapse.info.MessageReceiverStatus.RUNNING;
import static java.time.Duration.ofSeconds;
import static java.util.Arrays.asList;
import static java.util.Optional.of;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class StartupHealthIndicatorTest {


    @Test
    public void shouldInitiallyIndicateDownIfNoChannelsConfigured() {
        // given
        final StartupHealthIndicator healthCheck = new StartupHealthIndicator(Optional.empty());

        // when
        final Health health = healthCheck.health();

        // then
        assertThat(health.getStatus(), is(Status.UP));
    }

    @Test
    public void shouldInitiallyIndicateDownIfChannelsConfigured() {
        // given
        final List<EventSource> eventSources = asList(
                mockEventSource("some-stream"),
                mockEventSource("other-stream"));

        final StartupHealthIndicator healthCheck = new StartupHealthIndicator(Optional.of(eventSources));

        // when
        final Health health = healthCheck.health();
        
        // then
        assertThat(health.getStatus(), is(Status.DOWN));
    }

    @Test
    public void shouldIndicateDownBeforeLastChannelHasFinished_OneMessage() {
        // given
        final List<EventSource> eventSources = asList(
                mockEventSource("some-stream"),
                mockEventSource("other-stream"));
        final StartupHealthIndicator healthCheck = new StartupHealthIndicator(of(eventSources));
        
        // when
        whenRunning(healthCheck, "some-stream", ofSeconds(0));
        whenRunning(healthCheck, "other-stream", ofSeconds(11));

        // then
        final Health health = healthCheck.health();
        assertThat(health.getStatus(), is(Status.DOWN));
        assertThat(health.getDetails(), hasEntry("message", "Channel(s) not yet up to date"));
    }


    @Test
    public void shouldIndicateDownBeforeLastChannelHasFinishedWithAllShards() {
        // given
        final List<EventSource> eventSources = asList(
                mockEventSource("some-stream"),
                mockEventSource("other-stream"));
        final StartupHealthIndicator healthCheck = new StartupHealthIndicator(of(eventSources));

        // when
        whenRunning(healthCheck, "some-stream", ofSeconds(0));

        whenRunning(healthCheck, "other-stream", ofSeconds(2), ofSeconds(11));

        // then
        final Health health = healthCheck.health();
        assertThat(health.getStatus(), is(Status.DOWN));
        assertThat(health.getDetails(), hasEntry("message", "Channel(s) not yet up to date"));
    }

    @Test
    public void shouldIndicateUpIfLastChannelHasFinishedWithAllShards() {
        // given
        final List<EventSource> eventSources = asList(
                mockEventSource("some-stream"),
                mockEventSource("other-stream"));
        final StartupHealthIndicator healthCheck = new StartupHealthIndicator(of(eventSources));

        // when
        whenRunning(healthCheck, "some-stream", ofSeconds(0));

        whenRunning(healthCheck, "other-stream", ofSeconds(2), ofSeconds(9));

        // then
        final Health health = healthCheck.health();
        assertThat(health.getStatus(), is(Status.UP));
        assertThat(health.getDetails(), hasEntry("message", "All channels up to date"));
    }

    @Test
    public void shouldBeDownIfOneChannelStarted() {
        // given
        final List<EventSource> eventSources = asList(
                mockEventSource("some-stream"),
                mockEventSource("other-stream"));
        final StartupHealthIndicator healthCheck = new StartupHealthIndicator(of(eventSources));

        // when
        whenRunning(healthCheck, "some-stream", ofSeconds(0));


        // then
        final Health health = healthCheck.health();
        assertThat(health.getStatus(), is(Status.DOWN));
        assertThat(health.getDetails(), hasEntry("message", "Channel(s) not yet up to date"));
    }

    @Test
    public void shouldIgnoreFailedChannelsIfAllStarted() {
        // given
        final List<EventSource> eventSources = asList(
                mockEventSource("some-stream"),
                mockEventSource("other-stream"));
        final StartupHealthIndicator healthCheck = new StartupHealthIndicator(of(eventSources));

        // when
        whenRunning(healthCheck, "some-stream", ofSeconds(0));

        whenRunning(healthCheck, "other-stream", ofSeconds(0));
        whenFailed(healthCheck, "other-stream");

        // then
        final Health health = healthCheck.health();
        assertThat(health.getStatus(), is(Status.UP));
        assertThat(health.getDetails(), hasEntry("message", "All channels up to date"));
    }


    @Test
    public void shouldIndicateUpAfterLastChannelHasFinished() {
        // given
        final List<EventSource> eventSources = asList(
                mockEventSource("some-stream"),
                mockEventSource("other-stream"));
        final StartupHealthIndicator healthCheck = new StartupHealthIndicator(of(eventSources));

        // when
        whenRunning(healthCheck, "some-stream", ofSeconds(0));

        whenRunning(healthCheck, "other-stream", ofSeconds(9));

        // then
        final Health health = healthCheck.health();
        assertThat(health.getStatus(), is(Status.UP));
        assertThat(health.getDetails(), hasEntry("message", "All channels up to date"));
    }

    @Test
    public void shouldStayHealthyIfEndpointIsFallingBehind() {
        // given
        final List<EventSource> eventSources = asList(
                mockEventSource("some-stream"),
                mockEventSource("other-stream"));
        final StartupHealthIndicator healthCheck = new StartupHealthIndicator(of(eventSources));

        // when
        whenRunning(healthCheck, "some-stream", ofSeconds(0));

        whenRunning(healthCheck, "other-stream", ofSeconds(9));

        whenRunning(healthCheck, "some-stream", ofSeconds(11));
        whenRunning(healthCheck, "some-stream", ofSeconds(3600));

        // then
        final Health health = healthCheck.health();
        assertThat(health.getStatus(), is(Status.UP));
        assertThat(health.getDetails(), hasEntry("message", "All channels up to date"));
    }

    private EventSource mockEventSource(final String channelName) {
        EventSource eventSource = mock(EventSource.class);
        when(eventSource.getChannelName()).thenReturn(channelName);
        return eventSource;
    }

    private void whenRunning(final StartupHealthIndicator healthIndicator,
                             final String channelName,
                             final Duration... durationBehind) {
        if (durationBehind != null && durationBehind.length > 0) {
            final ChannelDurationBehind.Builder behind = channelDurationBehind();
            for (int i=0; i<durationBehind.length; ++i) {
                behind.with("some-shard-" + i, durationBehind[i]);
            }
            healthIndicator.on(builder()
                    .withChannelName(channelName)
                    .withStatus(RUNNING)
                    .withMessage("some message")
                    .withChannelDurationBehind(behind.build())
                    .build());
        } else {
            healthIndicator.on(builder()
                    .withChannelName(channelName)
                    .withStatus(RUNNING)
                    .withMessage("some message")
                    .withChannelDurationBehind(unknown()).build());
        }

    }

    private void whenFailed(final StartupHealthIndicator healthIndicator,
                            final String channelName) {
        healthIndicator.on(builder()
                .withChannelName(channelName)
                .withStatus(FAILED)
                .withMessage("some message").build());
    }

}
