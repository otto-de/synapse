package de.otto.synapse.configuration;

import de.otto.synapse.endpoint.MessageInterceptor;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import org.junit.Test;

import static de.otto.synapse.endpoint.EndpointType.RECEIVER;
import static de.otto.synapse.endpoint.EndpointType.SENDER;
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
        assertThat(registry.getRegistrations("foo", RECEIVER), contains(matchingChannelsWith("foo", interceptor)));
        assertThat(registry.getRegistrations("foo", SENDER), contains(matchingChannelsWith("foo", interceptor)));
    }

    @Test
    public void shouldReturnInterceptorChainForRegisteredInterceptor() {
        // given
        MessageInterceptorRegistry registry = new MessageInterceptorRegistry();
        MessageInterceptor interceptor = mock(MessageInterceptor.class);
        // when
        registry.register(matchingChannelsWith("foo", interceptor));
        // then
        assertThat(registry.getInterceptorChain("foo", RECEIVER).getInterceptors(), contains(interceptor));
        assertThat(registry.getInterceptorChain("foo", SENDER).getInterceptors(), contains(interceptor));
        assertThat(registry.getInterceptorChain("bar", RECEIVER).getInterceptors(), is(empty()));
        assertThat(registry.getInterceptorChain("bar", SENDER).getInterceptors(), is(empty()));
    }

    @Test
    public void shouldRegisterInterceptorWithChannelNamePattern() {
        // given
        MessageInterceptorRegistry registry = new MessageInterceptorRegistry();
        MessageInterceptor interceptor = mock(MessageInterceptor.class);
        // when
        registry.register(matchingChannelsWith("foo.*", interceptor));
        // then
        assertThat(registry.getRegistrations("foo", RECEIVER), contains(matchingChannelsWith("foo.*", interceptor)));
        assertThat(registry.getRegistrations("foobar", SENDER), contains(matchingChannelsWith("foo.*", interceptor)));
        assertThat(registry.getRegistrations("bar", SENDER), is(empty()));
    }

    @Test
    public void shouldReturnInterceptorChainForRegisteredInterceptorsWithChannelNamePattern() {
        // given
        MessageInterceptorRegistry registry = new MessageInterceptorRegistry();
        MessageInterceptor interceptor = mock(MessageInterceptor.class);
        // when
        registry.register(matchingChannelsWith("fo.*", interceptor));
        // then
        assertThat(registry.getInterceptorChain("foo", SENDER).getInterceptors(), contains(interceptor));
        assertThat(registry.getInterceptorChain("foobar", SENDER).getInterceptors(), contains(interceptor));
        assertThat(registry.getInterceptorChain("bar", SENDER).getInterceptors(), is(empty()));
    }

    @Test
    public void shouldRegisterInterceptorWithEndpointType() {
        // given
        MessageInterceptorRegistry registry = new MessageInterceptorRegistry();
        MessageInterceptor interceptor = mock(MessageInterceptor.class);
        // when
        registry.register(matchingReceiverChannelsWith("foo", interceptor));
        // then
        assertThat(registry.getRegistrations("foo", RECEIVER), contains(
                matchingReceiverChannelsWith("foo", interceptor))
        );
        assertThat(registry.getRegistrations("foo", SENDER), is(empty()));
    }

    @Test
    public void shouldReturnInterceptorChainForRegisteredInterceptorWithEndpointType() {
        // given
        MessageInterceptorRegistry registry = new MessageInterceptorRegistry();
        MessageInterceptor interceptor = mock(MessageInterceptor.class);
        // when
        registry.register(matchingReceiverChannelsWith("foo", interceptor));
        // then
        assertThat(registry.getInterceptorChain("foo", RECEIVER).getInterceptors(), contains(interceptor));
        assertThat(registry.getInterceptorChain("foo", SENDER).getInterceptors(), is(empty()));
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
        assertThat(registry.getRegistrations("foo", RECEIVER), contains(
                matchingChannelsWith("foo", firstInterceptor),
                matchingChannelsWith("foo", secondInterceptor)));
    }

    @Test
    public void shouldReturnInterceptorChainForMultipleInterceptorsForSameChannel() {
        // given
        MessageInterceptorRegistry registry = new MessageInterceptorRegistry();
        MessageInterceptor firstInterceptor = mock(MessageInterceptor.class);
        MessageInterceptor secondInterceptor = mock(MessageInterceptor.class);
        // when
        registry.register(matchingChannelsWith("foo", firstInterceptor));
        registry.register(matchingChannelsWith("foo", secondInterceptor));
        // then
        assertThat(registry.getInterceptorChain("foo", RECEIVER).getInterceptors(), contains(firstInterceptor, secondInterceptor));
    }

    @Test
    public void shouldReturnEmptyListForMissingInterceptor() {
        // given
        MessageInterceptorRegistry registry = new MessageInterceptorRegistry();
        // when
        // then
        assertThat(registry.getRegistrations("foo", RECEIVER), is(empty()));
    }

    @Test
    public void shouldReturnEmptyInterceptorChainForMissingInterceptor() {
        // given
        MessageInterceptorRegistry registry = new MessageInterceptorRegistry();
        // when
        // then
        assertThat(registry.getInterceptorChain("foo", RECEIVER).getInterceptors(), is(empty()));
    }

}