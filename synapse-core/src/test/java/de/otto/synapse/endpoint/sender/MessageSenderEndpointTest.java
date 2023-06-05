package de.otto.synapse.endpoint.sender;

import de.otto.synapse.endpoint.MessageInterceptor;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.message.Key;
import de.otto.synapse.message.Message;
import de.otto.synapse.message.TextMessage;
import de.otto.synapse.translator.MessageTranslator;
import de.otto.synapse.translator.TextMessageTranslator;
import jakarta.annotation.Nonnull;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static de.otto.synapse.endpoint.MessageInterceptorRegistration.allChannelsWith;
import static de.otto.synapse.endpoint.MessageInterceptorRegistration.matchingChannelsWith;
import static de.otto.synapse.message.Message.message;
import static java.util.concurrent.CompletableFuture.completedFuture;
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
        final MessageTranslator<TextMessage> messageTranslator = (m) -> TextMessage.of((Message<String>)m);
        final MessageSenderEndpoint senderEndpoint = new AbstractMessageSenderEndpoint("test", new MessageInterceptorRegistry(), messageTranslator) {
            @Override
            public CompletableFuture<Void> doSend(final @Nonnull TextMessage message) {
                numMessagesSent.incrementAndGet();
                return completedFuture(null);
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
        final MessageTranslator<TextMessage> messageTranslator = mock(MessageTranslator.class);
        when(messageTranslator.apply(any(Message.class))).thenReturn(TextMessage.of("translated", null));
        final MessageSenderEndpoint senderEndpoint = new AbstractMessageSenderEndpoint("foo-channel", new MessageInterceptorRegistry(), messageTranslator) {
            @Override
            protected CompletableFuture<Void> doSend(final TextMessage message) { /* no-op */
                return completedFuture(null);
            }
        };
        // when
        final Message<Object> message = message("foo", null);
        senderEndpoint.send(message);
        // then
        verify(messageTranslator).apply(message);
    }

    @Test
    public void shouldRegisterMessageInterceptor() {
        // given
        final MessageTranslator<TextMessage> messageTranslator = mock(TextMessageTranslator.class);
        final MessageInterceptorRegistry registry = new MessageInterceptorRegistry();
        final MessageSenderEndpoint senderEndpoint = new AbstractMessageSenderEndpoint("foo-channel", registry, messageTranslator) {
            @Override
            protected CompletableFuture<Void> doSend(TextMessage message) { /* no-op */
                return completedFuture(null);
            }
        };
        // when
        MessageInterceptor interceptor = (m) -> TextMessage.of("intercepted", null);
        registry.register(allChannelsWith(interceptor));
        // then
        assertThat(senderEndpoint.getInterceptorChain().getInterceptors(), Matchers.contains(interceptor));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldInterceptMessages() {
        // given
        final MessageTranslator<TextMessage> messageTranslator = (m) -> TextMessage.of((Message<String>)m);
        final MessageInterceptor interceptor = mock(MessageInterceptor.class);
        final MessageInterceptorRegistry registry = new MessageInterceptorRegistry();
        registry.register(matchingChannelsWith("foo-channel", interceptor));
        final MessageSenderEndpoint senderEndpoint = new AbstractMessageSenderEndpoint("foo-channel", registry, messageTranslator) {
            @Override
            protected CompletableFuture<Void> doSend(@Nonnull TextMessage message) { /* no-op */
                return completedFuture(null);
            }
        };
        // when
        final Message<String> message = message("foo", null);
        senderEndpoint.send(message);
        // then
        verify(interceptor).intercept(TextMessage.of(message));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldDropFilteredMessages() {
        // given
        final MessageTranslator<TextMessage> messageTranslator = (m) -> TextMessage.of((Message<String>)m);
        final MessageInterceptor interceptor = mock(MessageInterceptor.class);
        when(interceptor.intercept(any(TextMessage.class))).thenReturn(null);
        final MessageInterceptorRegistry registry = new MessageInterceptorRegistry();
        registry.register(matchingChannelsWith("foo-channel", interceptor));
        final MessageSenderEndpoint senderEndpoint = new AbstractMessageSenderEndpoint("foo-channel", registry, messageTranslator) {
            @Override
            protected CompletableFuture<Void> doSend(@Nonnull TextMessage message) {
                fail("This should not be called for dropped messages!");
                return completedFuture(null);
            }
        };
        // when
        final Message<String> message = message("foo", null);
        senderEndpoint.send(message);
        // then
        verify(interceptor).intercept(TextMessage.of(message));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldTranslateAndSendMessagesWithoutInterceptors() {
        // given
        final MessageTranslator<TextMessage> messageTranslator = (m) -> TextMessage.of(m.getKey(), "translated");

        final AtomicReference<TextMessage> sentMessage = new AtomicReference<>(null);
        final MessageSenderEndpoint senderEndpoint = new AbstractMessageSenderEndpoint("foo-channel", new MessageInterceptorRegistry(), messageTranslator) {
            @Override
            protected CompletableFuture<Void> doSend(@Nonnull TextMessage message) {
                sentMessage.set(message);
                return completedFuture(null);
            }
        };

        // when
        senderEndpoint.send(message("foo", ""));

        // then
        assertThat(sentMessage.get().getKey(), is(Key.of("foo")));
        assertThat(sentMessage.get().getPayload(), is("translated"));
    }

    @Test
    public void shouldSendTranslatedAndInterceptedMessage() {
        // given
        final MessageTranslator<TextMessage> messageTranslator = (m) -> TextMessage.of(m.getKey(), "translated ");
        final MessageInterceptor interceptor = (m) -> TextMessage.of(m.getKey(), m.getPayload() + "and intercepted");
        final MessageInterceptorRegistry registry = new MessageInterceptorRegistry();
        registry.register(matchingChannelsWith("foo-channel", interceptor));

        final AtomicReference<TextMessage> sentMessage = new AtomicReference<>(null);
        final MessageSenderEndpoint senderEndpoint = new AbstractMessageSenderEndpoint("foo-channel", registry, messageTranslator) {
            @Override
            protected CompletableFuture<Void> doSend(@Nonnull TextMessage message) {
                sentMessage.set(message);
                return completedFuture(null);
            }
        };

        // when
        senderEndpoint.send(message("foo", ""));

        // then
        assertThat(sentMessage.get().getKey(), is(Key.of("foo")));
        assertThat(sentMessage.get().getPayload(), is("translated and intercepted"));
    }

    @Test
    public void shouldSendTranslatedAndInterceptedMessageBatch() {
        // given
        final MessageTranslator<TextMessage> messageTranslator = (m) -> TextMessage.of(m.getKey(), "translated ");
        final MessageInterceptor interceptor = (m) -> TextMessage.of(m.getKey(), m.getPayload() + "and intercepted");
        final MessageInterceptorRegistry registry = new MessageInterceptorRegistry();
        registry.register(matchingChannelsWith("foo-channel", interceptor));

        final List<TextMessage> sentMessages = new ArrayList<>();
        final MessageSenderEndpoint senderEndpoint = new AbstractMessageSenderEndpoint("foo-channel", registry, messageTranslator) {
            @Override
            protected CompletableFuture<Void> doSend(@Nonnull TextMessage message) {
                sentMessages.add(message);
                return completedFuture(null);
            }
        };
        // when
        senderEndpoint.sendBatch(Stream.of(message("foo", ""), message("bar", "")));

        // then
        assertThat(sentMessages.get(0).getKey(), is(Key.of("foo")));
        assertThat(sentMessages.get(0).getPayload(), is("translated and intercepted"));
        assertThat(sentMessages.get(1).getKey(), is(Key.of("bar")));
        assertThat(sentMessages.get(1).getPayload(), is("translated and intercepted"));
    }

}