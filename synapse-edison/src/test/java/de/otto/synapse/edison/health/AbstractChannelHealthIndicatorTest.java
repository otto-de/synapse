package de.otto.synapse.edison.health;

import de.otto.synapse.channel.ChannelDurationBehind;
import org.junit.Test;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

import static de.otto.synapse.channel.ChannelDurationBehind.channelDurationBehind;
import static de.otto.synapse.channel.ChannelDurationBehind.unknown;
import static de.otto.synapse.info.MessageReceiverNotification.builder;
import static de.otto.synapse.info.MessageReceiverStatus.FAILED;
import static de.otto.synapse.info.MessageReceiverStatus.RUNNING;
import static java.time.Duration.ofSeconds;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public abstract class AbstractChannelHealthIndicatorTest {

    @Test
    public void shouldInitiallyIndicateDownIfNoChannelsConfigured() {
        // given
        final AbstractChannelHealthIndicator healthCheck = createHealthIndicator(Collections.emptyList());

        // when
        final Health health = healthCheck.health();

        // then
        assertThat(health.getStatus(), is(Status.UP));
    }

    @Test
    public void shouldInitiallyIndicateDownIfChannelsConfigured() {
        // given
        final List<String> eventSourceNames = asList("some-stream","other-stream");

        final AbstractChannelHealthIndicator healthCheck = createHealthIndicator(eventSourceNames);

        // when
        final Health health = healthCheck.health();

        // then
        assertThat(health.getStatus(), is(Status.DOWN));
    }

    @Test
    public void shouldIndicateDownBeforeLastChannelHasFinished_OneMessage() {
        // given
        final List<String> eventSourceNames = asList("some-stream","other-stream");
        final AbstractChannelHealthIndicator healthCheck = createHealthIndicator(eventSourceNames);

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
        final List<String> eventSourceNames = asList("some-stream","other-stream");
        final AbstractChannelHealthIndicator healthCheck = createHealthIndicator(eventSourceNames);

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
        final List<String> eventSourceNames = asList("some-stream","other-stream");
        final AbstractChannelHealthIndicator healthCheck = createHealthIndicator(eventSourceNames);

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
        final List<String> eventSourceNames = asList("some-stream","other-stream");
        final AbstractChannelHealthIndicator healthCheck = createHealthIndicator(eventSourceNames);

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
        final List<String> eventSourceNames = asList("some-stream","other-stream");
        final AbstractChannelHealthIndicator healthCheck = createHealthIndicator(eventSourceNames);

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
        final List<String> eventSourceNames = asList("some-stream","other-stream");
        final AbstractChannelHealthIndicator healthCheck = createHealthIndicator(eventSourceNames);

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
        final List<String> eventSourceNames = asList("some-stream","other-stream");
        final AbstractChannelHealthIndicator healthCheck = createHealthIndicator(eventSourceNames);

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

    private void whenRunning(final AbstractChannelHealthIndicator healthIndicator,
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

    protected abstract AbstractChannelHealthIndicator createHealthIndicator(List<String> channelNames);

    private void whenFailed(final AbstractChannelHealthIndicator healthIndicator,
                            final String channelName) {
        healthIndicator.on(builder()
                .withChannelName(channelName)
                .withStatus(FAILED)
                .withMessage("some message").build());
    }
}