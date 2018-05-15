package de.otto.synapse.configuration;

import de.otto.synapse.endpoint.EndpointType;
import de.otto.synapse.endpoint.MessageInterceptor;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import org.junit.Test;

import static de.otto.synapse.endpoint.MessageInterceptorRegistration.matchingChannelsWith;
import static de.otto.synapse.endpoint.MessageInterceptorRegistration.matchingReceiverChannelsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.mock;

public class MessageInterceptorRegistryTest {

    @Test
    public void shouldRegisterInterceptor() {
        // given
        MessageInterceptorRegistry registry = new MessageInterceptorRegistry();
        MessageInterceptor interceptor = mock(MessageInterceptor.class);
        // when
        registry.register(matchingChannelsWith("foo", interceptor));
        // then
        assertThat(registry.getRegistrations("foo", EndpointType.RECEIVER), contains(matchingChannelsWith("foo", interceptor)));
    }

    @Test
    public void shouldRegisterInterceptorWithChannelNamePattern() {
        // given
        MessageInterceptorRegistry registry = new MessageInterceptorRegistry();
        MessageInterceptor interceptor = mock(MessageInterceptor.class);
        // when
        registry.register(matchingChannelsWith("foo.*", interceptor));
        // then
        assertThat(registry.getRegistrations("foo", EndpointType.RECEIVER), contains(matchingChannelsWith("foo.*", interceptor)));
        assertThat(registry.getRegistrations("foobar", EndpointType.SENDER), contains(matchingChannelsWith("foo.*", interceptor)));
        assertThat(registry.getRegistrations("bar", EndpointType.SENDER), is(empty()));
    }

    @Test
    public void shouldRegisterInterceptorWithEndpointType() {
        // given
        MessageInterceptorRegistry registry = new MessageInterceptorRegistry();
        MessageInterceptor interceptor = mock(MessageInterceptor.class);
        // when
        registry.register(matchingReceiverChannelsWith("foo", interceptor));
        // then
        assertThat(registry.getRegistrations("foo", EndpointType.RECEIVER), contains(
                matchingReceiverChannelsWith("foo", interceptor))
        );
        assertThat(registry.getRegistrations("foo", EndpointType.SENDER), is(empty()));
    }

    @Test
    public void shouldRegisterMultipleInterceptorsForSameChannel() {
        // given
        MessageInterceptorRegistry registry = new MessageInterceptorRegistry();
        MessageInterceptor firstInterceptor = mock(MessageInterceptor.class);
        MessageInterceptor secondInterceptor = mock(MessageInterceptor.class);
        // when
        registry.register(matchingChannelsWith("foo", firstInterceptor));
        registry.register(matchingChannelsWith("foo", secondInterceptor));
        // then
        assertThat(registry.getRegistrations("foo", EndpointType.RECEIVER), contains(
                matchingChannelsWith("foo", firstInterceptor),
                matchingChannelsWith("foo", secondInterceptor)));
    }

    @Test
    public void shouldReturnEmptyListForMissingInterceptor() {
        // given
        MessageInterceptorRegistry registry = new MessageInterceptorRegistry();
        // when
        // then
        assertThat(registry.getRegistrations("foo", EndpointType.RECEIVER), is(empty()));
    }

}