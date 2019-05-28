package de.otto.synapse.endpoint;

import de.otto.synapse.message.TextMessage;
import org.junit.Test;

import javax.annotation.Nonnull;

import static de.otto.synapse.endpoint.EndpointType.SENDER;
import static de.otto.synapse.endpoint.MessageInterceptorRegistration.senderChannelsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class MessageEndpointTest {

    /*
       Should be a NullPointerException. Actually, Intellij will violate the @Nonnull contract and
       evaluate it at runtime - by throwing an IllegalArgumentException. Which is wrong. Behaviour
       of Intellij can be disabled, but I'm too lazy to figure out how to do it with 'gradle idea'
     */

    @Test(expected = RuntimeException.class)
    public void shouldFailToCreateWithNullChannelName() {
        new AbstractMessageEndpoint(null, new MessageInterceptorRegistry()) {
            @Nonnull
            @Override
            public EndpointType getEndpointType() {
                return SENDER;
            }
        };
    }
    @Test
    public void shouldReturnChannelName() {
        final AbstractMessageEndpoint endpoint = new AbstractMessageEndpoint("foo", new MessageInterceptorRegistry()) {
            @Nonnull
            @Override
            public EndpointType getEndpointType() {
                return SENDER;
            }
        };
        assertThat(endpoint.getChannelName(), is("foo"));
    }


    /*
       Should be a NullPointerException. Actually, Intellij will violate the @Nonnull contract and
       evaluate it at runtime - by throwing an IllegalArgumentException. Which is wrong. Behaviour
       of Intellij can be disabled, but I'm too lazy to figure out how to do it with 'gradle idea'
     */
    @Test(expected = RuntimeException.class)
    public void shouldFailToRegisterFromNullRegistry() {
        new AbstractMessageEndpoint("foo", null) {
            @Nonnull
            @Override
            public EndpointType getEndpointType() {
                return SENDER;
            }
        };
    }

    @Test
    public void shouldInterceptMessages() {
        final MessageInterceptor interceptor = mock(MessageInterceptor.class);
        MessageInterceptorRegistry registry = new MessageInterceptorRegistry();
        final AbstractMessageEndpoint endpoint = new AbstractMessageEndpoint("foo", registry) {
            @Nonnull
            @Override
            public EndpointType getEndpointType() {
                return SENDER;
            }
        };
        registry.register(senderChannelsWith(interceptor));
        final TextMessage message = mock(TextMessage.class);
        endpoint.intercept(message);
        verify(interceptor).intercept(message);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldReturnMessageWithoutInterceptor() {
        final AbstractMessageEndpoint messageEndpoint = new AbstractMessageEndpoint("foo", new MessageInterceptorRegistry()) {
            @Nonnull
            @Override
            public EndpointType getEndpointType() {
                return SENDER;
            }
        };
        final TextMessage message = mock(TextMessage.class);
        assertThat(messageEndpoint.intercept(message), is(message));
    }
}