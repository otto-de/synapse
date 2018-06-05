package de.otto.synapse.endpoint;

import de.otto.synapse.message.Message;
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
        new AbstractMessageEndpoint(null) {
            @Nonnull
            @Override
            public EndpointType getEndpointType() {
                return SENDER;
            }
        };
    }
    @Test
    public void shouldReturnChannelName() {
        final AbstractMessageEndpoint endpoint = new AbstractMessageEndpoint("foo") {
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
        AbstractMessageEndpoint messageEndpoint = new AbstractMessageEndpoint("foo") {
            @Nonnull
            @Override
            public EndpointType getEndpointType() {
                return SENDER;
            }
        };
        messageEndpoint.registerInterceptorsFrom(null);
    }

    @Test
    public void shouldInterceptMessages() {
        final MessageInterceptor interceptor = mock(MessageInterceptor.class);
        final AbstractMessageEndpoint endpoint = new AbstractMessageEndpoint("foo") {
            @Nonnull
            @Override
            public EndpointType getEndpointType() {
                return SENDER;
            }
        };
        MessageInterceptorRegistry registry = new MessageInterceptorRegistry();
        registry.register(senderChannelsWith(interceptor));
        endpoint.registerInterceptorsFrom(registry);
        final Message<String> message = mock(Message.class);
        endpoint.intercept(message);
        verify(interceptor).intercept(message);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldReturnMessageWithoutInterceptor() {
        final AbstractMessageEndpoint messageEndpoint = new AbstractMessageEndpoint("foo") {
            @Nonnull
            @Override
            public EndpointType getEndpointType() {
                return SENDER;
            }
        };
        final Message<String> message = mock(Message.class);
        assertThat(messageEndpoint.intercept(message), is(message));
    }
}