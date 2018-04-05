package de.otto.synapse.endpoint;

import de.otto.synapse.message.Message;
import org.junit.Test;

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
        new MessageEndpoint(null) {};
    }
    @Test
    public void shouldReturnChannelName() {
        final MessageEndpoint endpoint = new MessageEndpoint("foo") {};
        assertThat(endpoint.getChannelName(), is("foo"));
    }


    /*
       Should be a NullPointerException. Actually, Intellij will violate the @Nonnull contract and
       evaluate it at runtime - by throwing an IllegalArgumentException. Which is wrong. Behaviour
       of Intellij can be disabled, but I'm too lazy to figure out how to do it with 'gradle idea'
     */
    @Test(expected = RuntimeException.class)
    public void shouldFailToRegisterNullInterceptor() {
        MessageEndpoint messageEndpoint = new MessageEndpoint("foo") {};
        messageEndpoint.register(null);
    }

    @Test
    public void shouldInterceptMessages() {
        final MessageInterceptor interceptor = mock(MessageInterceptor.class);
        final MessageEndpoint endpoint = new MessageEndpoint("foo") {};
        endpoint.register(interceptor);
        final Message<String> message = mock(Message.class);
        endpoint.intercept(message);
        verify(interceptor).intercept(message);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldReturnMessageWithoutInterceptor() {
        final MessageEndpoint messageEndpoint = new MessageEndpoint("foo") {};
        final Message<String> message = mock(Message.class);
        assertThat(messageEndpoint.intercept(message), is(message));
    }
}