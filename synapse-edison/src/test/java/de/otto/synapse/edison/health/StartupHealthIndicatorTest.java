package de.otto.synapse.edison.health;

import com.google.common.collect.ImmutableMap;
import de.otto.edison.testsupport.util.TestClock;
import de.otto.synapse.edison.provider.MessageReceiverEndpointInfoProvider;
import de.otto.synapse.eventsource.EventSource;
import de.otto.synapse.info.MessageEndpointNotification;
import de.otto.synapse.info.MessageEndpointStatus;
import org.junit.Test;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import java.util.Optional;

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

        EventSource mockEventSource = mock(EventSource.class);
        when(mockEventSource.getChannelName()).thenReturn("some-stream");

        // when
        provider.onEventSourceNotification(MessageEndpointNotification.builder().withChannelName("some-stream").withStatus(MessageEndpointStatus.STARTING).build());
        provider.onEventSourceNotification(MessageEndpointNotification.builder().withChannelName("some-stream").withStatus(MessageEndpointStatus.STARTED).build());
        provider.onEventSourceNotification(MessageEndpointNotification.builder().withChannelName("some-stream").withStatus(MessageEndpointStatus.RUNNING).build());
        provider.onEventSourceNotification(MessageEndpointNotification.builder().withChannelName("some-stream").withStatus(MessageEndpointStatus.FINISHED).build());

        provider.onEventSourceNotification(MessageEndpointNotification.builder().withChannelName("other-stream").withStatus(MessageEndpointStatus.STARTING).build());
        provider.onEventSourceNotification(MessageEndpointNotification.builder().withChannelName("other-stream").withStatus(MessageEndpointStatus.STARTED).build());
        provider.onEventSourceNotification(MessageEndpointNotification.builder().withChannelName("other-stream").withStatus(MessageEndpointStatus.RUNNING).build());

        // then
        Health health = healthCheck.health();
        assertThat(health.getStatus(), is(Status.DOWN));
        assertThat(health.getDetails(), hasEntry("some-stream", ImmutableMap.of("message", "Channel at HEAD position", "status", "HEAD")));
        assertThat(health.getDetails(), hasEntry("other-stream", ImmutableMap.of("message", "Channel not yet finished", "status", "BEHIND")));

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
        provider.onEventSourceNotification(MessageEndpointNotification.builder().withChannelName("some-stream").withStatus(MessageEndpointStatus.STARTING).build());
        provider.onEventSourceNotification(MessageEndpointNotification.builder().withChannelName("some-stream").withStatus(MessageEndpointStatus.STARTED).build());
        provider.onEventSourceNotification(MessageEndpointNotification.builder().withChannelName("some-stream").withStatus(MessageEndpointStatus.RUNNING).build());
        provider.onEventSourceNotification(MessageEndpointNotification.builder().withChannelName("some-stream").withStatus(MessageEndpointStatus.FINISHED).build());

        provider.onEventSourceNotification(MessageEndpointNotification.builder().withChannelName("other-stream").withStatus(MessageEndpointStatus.STARTING).build());
        provider.onEventSourceNotification(MessageEndpointNotification.builder().withChannelName("other-stream").withStatus(MessageEndpointStatus.STARTED).build());
        provider.onEventSourceNotification(MessageEndpointNotification.builder().withChannelName("other-stream").withStatus(MessageEndpointStatus.RUNNING).build());
        provider.onEventSourceNotification(MessageEndpointNotification.builder().withChannelName("other-stream").withStatus(MessageEndpointStatus.FINISHED).build());

        // then
        Health health = healthCheck.health();
        assertThat(health.getStatus(), is(Status.UP));
        assertThat(health.getDetails(), hasEntry("some-stream", ImmutableMap.of("message", "Channel at HEAD position", "status", "HEAD")));
        assertThat(health.getDetails(), hasEntry("other-stream", ImmutableMap.of("message", "Channel at HEAD position", "status", "HEAD")));
    }

    private EventSource mockEventSource(String channelName) {
        EventSource eventSource = mock(EventSource.class);
        when(eventSource.getChannelName()).thenReturn(channelName);
        return eventSource;
    }

}
