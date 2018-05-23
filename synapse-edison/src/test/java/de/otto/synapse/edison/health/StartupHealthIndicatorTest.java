package de.otto.synapse.edison.health;

import de.otto.edison.testsupport.util.TestClock;
import de.otto.synapse.channel.ShardPosition;
import de.otto.synapse.edison.provider.MessageReceiverEndpointInfoProvider;
import de.otto.synapse.eventsource.EventSource;
import de.otto.synapse.info.MessageEndpointStatus;
import org.junit.Test;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static de.otto.synapse.channel.ChannelPosition.channelPosition;
import static de.otto.synapse.channel.ChannelPosition.fromHorizon;
import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static de.otto.synapse.info.MessageEndpointNotification.Builder;
import static de.otto.synapse.info.MessageEndpointNotification.builder;
import static de.otto.synapse.info.MessageEndpointStatus.*;
import static java.time.Duration.ofSeconds;
import static java.util.Arrays.asList;
import static java.util.Optional.of;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class StartupHealthIndicatorTest {

    private TestClock clock = TestClock.now();


    @Test
    public void shouldInitiallyIndicateDownIfNoChannelsConfigured() {
        // given
        MessageReceiverEndpointInfoProvider provider = new MessageReceiverEndpointInfoProvider(Optional.empty(), clock);
        StartupHealthIndicator healthCheck = new StartupHealthIndicator(provider);

        // when
        Health health = healthCheck.health();

        // then
        assertThat(health.getStatus(), is(Status.UP));
    }

    @Test
    public void shouldInitiallyIndicateDownIfChannelsConfigured() {
        // given
        MessageReceiverEndpointInfoProvider provider = new MessageReceiverEndpointInfoProvider(
                of(asList(
                        mockEventSource("some-stream"),
                        mockEventSource("other-stream"))),
                clock);

        StartupHealthIndicator healthCheck = new StartupHealthIndicator(provider);

        // when
        Health health = healthCheck.health();
        // then
        assertThat(health.getStatus(), is(Status.DOWN));
    }

    @Test
    public void shouldIndicateDownBeforeLastChannelHasFinished_OneMessage() {
        // given
        MessageReceiverEndpointInfoProvider provider = new MessageReceiverEndpointInfoProvider(
                of(asList(
                        mockEventSource("some-stream"),
                        mockEventSource("other-stream"))),
                clock);

        StartupHealthIndicator healthCheck = new StartupHealthIndicator(provider);

        // when
        whenStarting(provider, "some-stream");
        whenStarted(provider, "some-stream");
        whenRunning(provider, "some-stream", ofSeconds(0));
        whenFinished(provider, "some-stream");

        whenStarting(provider, "other-stream");
        whenStarted(provider, "other-stream");
        whenRunning(provider, "other-stream", ofSeconds(10));

        // then
        Health health = healthCheck.health();
        assertThat(health.getStatus(), is(Status.DOWN));
        assertThat(health.getDetails(), hasEntry("message", "Channels not yet up to date"));
    }


    @Test
    public void shouldIndicateDownBeforeLastChannelHasFinishedWithAllShards() {
        // given
        MessageReceiverEndpointInfoProvider provider = new MessageReceiverEndpointInfoProvider(
                of(asList(
                        mockEventSource("some-stream"),
                        mockEventSource("other-stream"))),
                clock);

        StartupHealthIndicator healthCheck = new StartupHealthIndicator(provider);

        // when
        whenStarting(provider, "some-stream");
        whenStarted(provider, "some-stream");
        whenRunning(provider, "some-stream", ofSeconds(0));
        whenFinished(provider, "some-stream");

        whenStarting(provider, "other-stream");
        whenStarted(provider, "other-stream");
        whenRunning(provider, "other-stream", ofSeconds(2), ofSeconds(11));

        // then
        Health health = healthCheck.health();
        assertThat(health.getStatus(), is(Status.DOWN));
        assertThat(health.getDetails(), hasEntry("message", "Channels not yet up to date"));
    }

    @Test
    public void shouldIndicateUpIfLastChannelHasFinishedWithAllShards() {
        // given
        MessageReceiverEndpointInfoProvider provider = new MessageReceiverEndpointInfoProvider(
                of(asList(
                        mockEventSource("some-stream"),
                        mockEventSource("other-stream"))),
                clock);

        StartupHealthIndicator healthCheck = new StartupHealthIndicator(provider);

        // when
        whenStarting(provider, "some-stream");
        whenStarted(provider, "some-stream");
        whenRunning(provider, "some-stream", ofSeconds(0));
        whenFinished(provider, "some-stream");

        whenStarting(provider, "other-stream");
        whenStarted(provider, "other-stream");
        whenRunning(provider, "other-stream", ofSeconds(2), ofSeconds(9));

        // then
        Health health = healthCheck.health();
        assertThat(health.getStatus(), is(Status.UP));
        assertThat(health.getDetails(), hasEntry("message", "All channels up to date"));
    }

    @Test
    public void shouldBeDownIfChannelStarted() {
        // given
        MessageReceiverEndpointInfoProvider provider = new MessageReceiverEndpointInfoProvider(
                of(asList(
                        mockEventSource("some-stream"),
                        mockEventSource("other-stream"))),
                clock);

        StartupHealthIndicator healthCheck = new StartupHealthIndicator(provider);

        // when
        whenStarting(provider, "some-stream");
        whenStarted(provider, "some-stream");
        whenRunning(provider, "some-stream", ofSeconds(0));

        whenStarting(provider, "other-stream");
        whenStarted(provider, "other-stream");

        // then
        Health health = healthCheck.health();
        assertThat(health.getStatus(), is(Status.DOWN));
        assertThat(health.getDetails(), hasEntry("message", "Channels not yet up to date"));
    }

    @Test
    public void shouldIgnoreFailedChannelsIfAllStarted() {
        // given
        MessageReceiverEndpointInfoProvider provider = new MessageReceiverEndpointInfoProvider(
                of(asList(
                        mockEventSource("some-stream"),
                        mockEventSource("other-stream"))),
                clock);

        StartupHealthIndicator healthCheck = new StartupHealthIndicator(provider);

        // when
        whenStarting(provider, "some-stream");
        whenStarted(provider, "some-stream");
        whenRunning(provider, "some-stream", ofSeconds(0));

        whenStarting(provider, "other-stream");
        whenStarted(provider, "other-stream");
        whenRunning(provider, "other-stream", ofSeconds(0));
        whenFailed(provider, "other-stream");

        // then
        Health health = healthCheck.health();
        assertThat(health.getStatus(), is(Status.UP));
        assertThat(health.getDetails(), hasEntry("message", "All channels up to date"));
    }


    @Test
    public void shouldIndicateUpAfterLastChannelHasFinished() {
        // given
        MessageReceiverEndpointInfoProvider provider = new MessageReceiverEndpointInfoProvider(
                of(asList(
                        mockEventSource("some-stream"),
                        mockEventSource("other-stream"))),
                clock);

        StartupHealthIndicator healthCheck = new StartupHealthIndicator(provider);

        EventSource mockEventSource = mock(EventSource.class);
        when(mockEventSource.getChannelName()).thenReturn("some-stream");

        // when
        whenStarting(provider, "some-stream");
        whenStarted(provider, "some-stream");
        whenRunning(provider, "some-stream", ofSeconds(0));
        whenFinished(provider, "some-stream");

        whenStarting(provider, "other-stream");
        whenStarted(provider, "other-stream");
        whenRunning(provider, "other-stream", ofSeconds(9));
        whenFinished(provider, "other-stream");

        // then
        Health health = healthCheck.health();
        assertThat(health.getStatus(), is(Status.UP));
        assertThat(health.getDetails(), hasEntry("message", "All channels up to date"));
    }

    private EventSource mockEventSource(final String channelName) {
        EventSource eventSource = mock(EventSource.class);
        when(eventSource.getChannelName()).thenReturn(channelName);
        return eventSource;
    }

    private void whenStarting(final MessageReceiverEndpointInfoProvider provider,
                              final String channelName) {
        sendNotification(provider, channelName, STARTING);
    }

    private void whenStarted(final MessageReceiverEndpointInfoProvider provider,
                             final String channelName) {
        sendNotification(provider, channelName, STARTED);
    }

    private void whenFinished(final MessageReceiverEndpointInfoProvider provider,
                              final String channelName) {
        sendNotification(provider, channelName, FINISHED);
    }

    private void whenRunning(final MessageReceiverEndpointInfoProvider provider,
                             final String channelName,
                             final Duration... durationBehind) {
        sendNotification(provider, channelName, RUNNING, durationBehind);
    }

    private void whenFailed(final MessageReceiverEndpointInfoProvider provider,
                            final String channelName) {
        sendNotification(provider, channelName, FAILED);
    }

    private void sendNotification(final MessageReceiverEndpointInfoProvider provider,
                                  final String channelName,
                                  final MessageEndpointStatus status,
                                  final Duration... durationBehind) {
        final Builder builder = builder()
                .withChannelName(channelName)
                .withStatus(status)
                .withMessage("some message");
        if (durationBehind != null && durationBehind.length > 0) {
            final List<ShardPosition> positions = new ArrayList<>();
            for (int i=0; i<durationBehind.length; ++i) {
                positions.add(fromPosition("some-shard-" + i, durationBehind[i], "42"));
            }
            builder.withChannelPosition(channelPosition(positions));
        } else {
            builder.withChannelPosition(fromHorizon());
        }
        provider.onEventSourceNotification(builder
                .build());
    }

}
