package de.otto.synapse.endpoint.sender;

import de.otto.synapse.endpoint.MessageInterceptor;
import de.otto.synapse.message.Message;
import de.otto.synapse.translator.MessageTranslator;
import org.junit.Test;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static de.otto.synapse.message.Message.message;
import static java.util.stream.Stream.of;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class MessageSenderEndpointTest {

    @Test
    @SuppressWarnings("unchecked")
    public void shouldCallSendMessageForBatch() {
        final AtomicInteger numMessagesSent = new AtomicInteger(0);
        final MessageSenderEndpoint senderEndpoint = new MessageSenderEndpoint("test", (m) -> (Message<String>)m) {
            @Override
            public void doSend(final @Nonnull Message<String> message) {
                numMessagesSent.incrementAndGet();
            }
        };
        senderEndpoint.sendBatch(of(
                message("1", null),
                message("2", null),
                message("3", null)));
        assertThat(numMessagesSent.get(), is(3));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldTranslateMessages() {
        // given
        final MessageTranslator<String> messageTranslator = mock(MessageTranslator.class);
        final MessageInterceptor interceptor = (m) -> m;
        final MessageSenderEndpoint senderEndpoint = new MessageSenderEndpoint("foo-channel", messageTranslator) {
            @Override
            protected void doSend(@Nonnull Message<String> message) { /* no-op */ }
        };
        senderEndpoint.register(interceptor);
        // when
        final Message<Object> message = message("foo", null);
        senderEndpoint.send(message);
        // then
        verify(messageTranslator).translate(message);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldInterceptMessages() {
        // given
        final MessageTranslator<String> messageTranslator = (m) -> (Message<String>) m;
        final MessageInterceptor interceptor = mock(MessageInterceptor.class);
        final MessageSenderEndpoint senderEndpoint = new MessageSenderEndpoint("foo-channel", messageTranslator) {
            @Override
            protected void doSend(@Nonnull Message<String> message) { /* no-op */ }
        };
        senderEndpoint.register(interceptor);
        // when
        final Message<String> message = message("foo", null);
        senderEndpoint.send(message);
        // then
        verify(interceptor).intercept(message);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldDropFilteredMessages() {
        // given
        final MessageTranslator<String> messageTranslator = (m) -> (Message<String>) m;
        final MessageInterceptor interceptor = mock(MessageInterceptor.class);
        when(interceptor.intercept(any(Message.class))).thenReturn(null);
        final MessageSenderEndpoint senderEndpoint = new MessageSenderEndpoint("foo-channel", messageTranslator) {
            @Override
            protected void doSend(@Nonnull Message<String> message) {
                fail("This should not be called for dropped messages!");
            }
        };
        senderEndpoint.register(interceptor);
        // when
        final Message<String> message = message("foo", null);
        senderEndpoint.send(message);
        // then
        verify(interceptor).intercept(message);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldTranslateAndSendMessagesWithoutInterceptors() {
        // given
        final MessageTranslator<String> messageTranslator = (m) -> message(m.getKey(), "translated");

        final AtomicReference<Message<String>> sentMessage = new AtomicReference<>(null);
        final MessageSenderEndpoint senderEndpoint = new MessageSenderEndpoint("foo-channel", messageTranslator) {
            @Override
            protected void doSend(@Nonnull Message<String> message) {
                sentMessage.set(message);
            }
        };

        // when
        senderEndpoint.send(message("foo", ""));

        // then
        assertThat(sentMessage.get().getKey(), is("foo"));
        assertThat(sentMessage.get().getPayload(), is("translated"));
    }

    @Test
    public void shouldSendTranslatedAndInterceptedMessage() {
        // given
        final MessageTranslator<String> messageTranslator = (m) -> message(m.getKey(), "translated ");
        final MessageInterceptor interceptor = (m) -> message(m.getKey(), m.getPayload() + "and intercepted");

        final AtomicReference<Message<String>> sentMessage = new AtomicReference<>(null);
        final MessageSenderEndpoint senderEndpoint = new MessageSenderEndpoint("foo-channel", messageTranslator) {
            @Override
            protected void doSend(@Nonnull Message<String> message) {
                sentMessage.set(message);
            }
        };
        senderEndpoint.register(interceptor);

        // when
        senderEndpoint.send(message("foo", ""));

        // then
        assertThat(sentMessage.get().getKey(), is("foo"));
        assertThat(sentMessage.get().getPayload(), is("translated and intercepted"));
    }

    @Test
    public void shouldSendTranslatedAndInterceptedMessageBatch() {
        // given
        final MessageTranslator<String> messageTranslator = (m) -> message(m.getKey(), "translated ");
        final MessageInterceptor interceptor = (m) -> message(m.getKey(), m.getPayload() + "and intercepted");

        final List<Message<String>> sentMessages = new ArrayList<>();
        final MessageSenderEndpoint senderEndpoint = new MessageSenderEndpoint("foo-channel", messageTranslator) {
            @Override
            protected void doSend(@Nonnull Message<String> message) {
                sentMessages.add(message);
            }
        };
        senderEndpoint.register(interceptor);
        // when
        senderEndpoint.sendBatch(Stream.of(message("foo", ""), message("bar", "")));

        // then
        assertThat(sentMessages.get(0).getKey(), is("foo"));
        assertThat(sentMessages.get(0).getPayload(), is("translated and intercepted"));
        assertThat(sentMessages.get(1).getKey(), is("bar"));
        assertThat(sentMessages.get(1).getPayload(), is("translated and intercepted"));
    }

}