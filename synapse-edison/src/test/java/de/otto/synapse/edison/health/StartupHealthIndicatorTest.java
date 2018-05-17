package de.otto.synapse.edison.health;

import com.google.common.collect.ImmutableMap;
import de.otto.synapse.endpoint.EndpointType;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.eventsource.EventSource;
import de.otto.synapse.message.Message;
import org.junit.Test;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import static de.otto.synapse.channel.ShardPosition.fromHorizon;
import static de.otto.synapse.message.Header.responseHeader;
import static de.otto.synapse.message.Message.message;
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
        MessageInterceptorRegistry registry = mock(MessageInterceptorRegistry.class);
        ChannelInfoProvider provider = new ChannelInfoProvider(Optional.empty(), registry);
        StartupHealthIndicator healthCheck = new StartupHealthIndicator(provider);

        // when
        Health health = healthCheck.health();

        // then
        assertThat(health.getStatus(), is(Status.UP));
    }

    @Test
    public void shouldInitiallyIndicateDownIfChannelsConfigured() {
        // given
        MessageInterceptorRegistry registry = mock(MessageInterceptorRegistry.class);
        ChannelInfoProvider provider = new ChannelInfoProvider(
                of(asList(
                    mockEventSource("some-stream"),
                    mockEventSource("other-stream"))),
                registry);

        StartupHealthIndicator healthCheck = new StartupHealthIndicator(provider);

        // when
        Health health = healthCheck.health();

        // then
        assertThat(health.getStatus(), is(Status.DOWN));
    }

    @Test
    public void shouldIndicateDownBeforeLastChannelHasFinished_OneMessage() {
        // given
        MessageInterceptorRegistry registry = new MessageInterceptorRegistry();
        ChannelInfoProvider provider = new ChannelInfoProvider(
                of(asList(
                        mockEventSource("some-stream"),
                        mockEventSource("other-stream"))),
                registry);

        StartupHealthIndicator healthCheck = new StartupHealthIndicator(provider);

        EventSource mockEventSource = mock(EventSource.class);
        when(mockEventSource.getChannelName()).thenReturn("some-stream");

        // when
        registry.getRegistrations("some-stream", EndpointType.RECEIVER).forEach(registration -> {
            final Message<String> headMessage = message("42", responseHeader(
                    fromHorizon(""),
                    Instant.now(),
                    Duration.ZERO), null);
            registration.getInterceptor().intercept(headMessage);
        });

        // then
        Health health = healthCheck.health();
        assertThat(health.getStatus(), is(Status.DOWN));
        assertThat(health.getDetails(), hasEntry("some-stream", ImmutableMap.of("message", "Channel at HEAD position", "status", "HEAD")));
        assertThat(health.getDetails(), hasEntry("other-stream", ImmutableMap.of("message", "Channel not yet finished", "status", "BEHIND")));

    }


    @Test
    public void shouldIndicateDownBeforeLastChannelHasFinished_TwoMessages() {
        // given
        MessageInterceptorRegistry registry = new MessageInterceptorRegistry();
        ChannelInfoProvider provider = new ChannelInfoProvider(
                of(asList(
                        mockEventSource("some-stream"),
                        mockEventSource("other-stream"))),
                registry);

        StartupHealthIndicator healthCheck = new StartupHealthIndicator(provider);

        EventSource mockEventSource = mock(EventSource.class);
        when(mockEventSource.getChannelName()).thenReturn("some-stream");

        // when
        registry.getRegistrations("some-stream", EndpointType.RECEIVER).forEach(registration -> {
            final Message<String> headMessage = message("42", responseHeader(
                    fromHorizon(""),
                    Instant.now(),
                    Duration.ZERO), null);
            registration.getInterceptor().intercept(headMessage);
        });
        registry.getRegistrations("other-stream", EndpointType.RECEIVER).forEach(registration -> {
            final Message<String> headMessage = message("42", responseHeader(
                    fromHorizon(""),
                    Instant.now(),
                    Duration.ofHours(1)), null);
            registration.getInterceptor().intercept(headMessage);
        });

        // then
        Health health = healthCheck.health();
        assertThat(health.getStatus(), is(Status.DOWN));
        assertThat(health.getDetails(), hasEntry("some-stream", ImmutableMap.of("message", "Channel at HEAD position", "status", "HEAD")));
        assertThat(health.getDetails(), hasEntry("other-stream", ImmutableMap.of("message", "Channel not yet finished", "status", "BEHIND")));

    }

    @Test
    public void shouldIndicateUpAfterLastChannelHasFinished() {
        // given
        MessageInterceptorRegistry registry = new MessageInterceptorRegistry();
        ChannelInfoProvider provider = new ChannelInfoProvider(
                of(asList(
                        mockEventSource("some-stream"),
                        mockEventSource("other-stream"))),
                registry);

        StartupHealthIndicator healthCheck = new StartupHealthIndicator(provider);

        EventSource mockEventSource = mock(EventSource.class);
        when(mockEventSource.getChannelName()).thenReturn("some-stream");

        // when
        registry.getRegistrations("some-stream", EndpointType.RECEIVER).forEach(registration -> {
            final Message<String> headMessage = message("42", responseHeader(
                    fromHorizon(""),
                    Instant.now(),
                    Duration.ZERO), null);
            registration.getInterceptor().intercept(headMessage);
        });
        registry.getRegistrations("other-stream", EndpointType.RECEIVER).forEach(registration -> {
            final Message<String> headMessage = message("42", responseHeader(
                    fromHorizon(""),
                    Instant.now(),
                    Duration.ZERO), null);
            registration.getInterceptor().intercept(headMessage);
        });

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
