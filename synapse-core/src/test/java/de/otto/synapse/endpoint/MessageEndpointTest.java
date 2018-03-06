package de.otto.synapse.endpoint;

import de.otto.synapse.message.Message;
import org.junit.Test;

import javax.annotation.Nonnull;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class MessageEndpointTest {

    @Test
    @SuppressWarnings("unchecked")
    public void defaultInterceptShouldReturnMessage() {
        final MessageEndpoint messageEndpoint = new MessageEndpoint() {
            @Nonnull
            @Override
            public String getChannelName() {
                return "foo";
            }
        };
        final Message<String> message = mock(Message.class);
        assertThat(messageEndpoint.intercept(message), is(message));
    }
}