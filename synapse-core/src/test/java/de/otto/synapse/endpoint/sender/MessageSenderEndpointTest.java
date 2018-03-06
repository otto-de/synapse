package de.otto.synapse.endpoint.sender;

import de.otto.synapse.message.Message;
import org.junit.Test;

import javax.annotation.Nonnull;
import java.util.concurrent.atomic.AtomicInteger;

import static de.otto.synapse.message.Message.message;
import static java.util.stream.Stream.of;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class MessageSenderEndpointTest {

    @Test
    public void shouldCallSendMessageForBatch() {
        final AtomicInteger numMessagesSent = new AtomicInteger(0);
        final MessageSenderEndpoint senderEndpoint = new MessageSenderEndpoint() {

            @Nonnull
            @Override
            public String getChannelName() {
                return "foo-channel";
            }

            @Override
            public <T> void send(final Message<T> message) {
                numMessagesSent.incrementAndGet();
            }
        };
        senderEndpoint.sendBatch(of(
                message("1", null),
                message("2", null),
                message("3", null)));
        assertThat(numMessagesSent.get(), is(3));
    }
}