package de.otto.synapse.eventsource;

import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.consumer.MessageDispatcher;
import de.otto.synapse.endpoint.InterceptorChain;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpoint;
import de.otto.synapse.message.Message;
import de.otto.synapse.messagestore.MessageStore;
import org.junit.Test;

import java.time.Instant;
import java.util.stream.Stream;

import static de.otto.synapse.channel.ChannelPosition.channelPosition;
import static de.otto.synapse.channel.ChannelPosition.fromHorizon;
import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static de.otto.synapse.message.Header.responseHeader;
import static de.otto.synapse.message.Message.message;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class CompactedEventSourceTest {

    @Test
    public void shouldReadMessagesFromMessageStore() {
        // given
        final MessageStore messageStore = mock(MessageStore.class);
        final CompactedEventSource eventSource = new CompactedEventSource("foo", messageStore, mock(MessageLogReceiverEndpoint.class));

        // when
        eventSource.consume();

        // then
        verify(messageStore).stream();
    }

    @Test
    public void shouldReadMessagesFromMessageLog() {
        // given
        final MessageStore messageStore = mock(MessageStore.class);
        when(messageStore.getLatestChannelPosition()).thenReturn(fromHorizon());
        final MessageLogReceiverEndpoint messageLog = mock(MessageLogReceiverEndpoint.class);
        final CompactedEventSource eventSource = new CompactedEventSource("foo", messageStore, messageLog);

        // when
        eventSource.consume();

        // then
        verify(messageLog).consumeUntil(fromHorizon(), Instant.MAX);
    }

    @Test
    public void shouldInterceptMessagesFromMessageStore() {
        // given
        final Instant arrivalTimestamp = Instant.now();
        // and some message store having a single message
        final MessageStore messageStore = mock(MessageStore.class);
        when(messageStore.stream()).thenReturn(Stream.of(message("1", responseHeader(null, arrivalTimestamp), null)));
        when(messageStore.getLatestChannelPosition()).thenReturn(fromHorizon());
        // and some MessageLogReceiverEndpoint with an InterceptorChain:
        final InterceptorChain interceptorChain = mock(InterceptorChain.class);
        final MessageLogReceiverEndpoint messageLog = mock(MessageLogReceiverEndpoint.class);
        when(messageLog.getInterceptorChain()).thenReturn(interceptorChain);
        // and our famous CompactedEventSource:
        final CompactedEventSource eventSource = new CompactedEventSource("foo", messageStore, messageLog);

        // when
        eventSource.consume();

        // then
        verify(interceptorChain).intercept(
                message("1", responseHeader(null, arrivalTimestamp), null)
        );
    }

    @Test
    public void shouldDropNullMessagesInterceptedFromMessageStore() {
        // given
        final Instant arrivalTimestamp = Instant.now();
        // and some message store having a single message
        final MessageStore messageStore = mock(MessageStore.class);
        when(messageStore.stream()).thenReturn(Stream.of(message("1", responseHeader(null, arrivalTimestamp), null)));
        when(messageStore.getLatestChannelPosition()).thenReturn(fromHorizon());
        // and some InterceptorChain that is dropping messages:
        final InterceptorChain interceptorChain = new InterceptorChain();
        interceptorChain.register((m)->null);
        // and some MessageLogReceiverEndpoint with our InterceptorChain:
        final MessageLogReceiverEndpoint messageLog = mock(MessageLogReceiverEndpoint.class);
        when(messageLog.getInterceptorChain()).thenReturn(interceptorChain);
        final MessageDispatcher messageDispatcher = mock(MessageDispatcher.class);
        when(messageLog.getMessageDispatcher()).thenReturn(messageDispatcher);
        // and our famous CompactedEventSource:
        final CompactedEventSource eventSource = new CompactedEventSource("foo", messageStore, messageLog);

        // when
        eventSource.consume();

        // then
        verify(messageDispatcher, never()).accept(any(Message.class));
    }

    @Test
    public void shouldDispatchMessagesFromMessageStore() {
        // given
        final Instant arrivalTimestamp = Instant.now();
        // and some message store having a single message
        final MessageStore messageStore = mock(MessageStore.class);
        when(messageStore.stream()).thenReturn(Stream.of(message("1", responseHeader(null, arrivalTimestamp), null)));
        when(messageStore.getLatestChannelPosition()).thenReturn(fromHorizon());
        // and some MessageLogReceiverEndpoint with our InterceptorChain:
        final MessageLogReceiverEndpoint messageLog = mock(MessageLogReceiverEndpoint.class);
        when(messageLog.getInterceptorChain()).thenReturn(new InterceptorChain());
        final MessageDispatcher messageDispatcher = mock(MessageDispatcher.class);
        when(messageLog.getMessageDispatcher()).thenReturn(messageDispatcher);
        // and our famous CompactedEventSource:
        final CompactedEventSource eventSource = new CompactedEventSource("foo", messageStore, messageLog);

        // when
        eventSource.consume();

        // then
        verify(messageDispatcher).accept(message("1", responseHeader(null, arrivalTimestamp), null));
    }

    @Test
    public void shouldContinueWithChannelPositionFromMessageStore() {
        // given
        final ChannelPosition expectedChannelPosition = channelPosition(fromPosition("bar", "42"));
        // and some message store having a single message
        final MessageStore messageStore = mock(MessageStore.class);
        when(messageStore.getLatestChannelPosition()).thenReturn(expectedChannelPosition);
        // and some MessageLogReceiverEndpoint with our InterceptorChain:
        final MessageLogReceiverEndpoint messageLog = mock(MessageLogReceiverEndpoint.class);
        // and our famous CompactedEventSource:
        final CompactedEventSource eventSource = new CompactedEventSource("foo", messageStore, messageLog);

        // when
        eventSource.consume();

        // then
        verify(messageLog).consumeUntil(expectedChannelPosition, Instant.MAX);
    }

    @Test
    public void shouldCloseMessageStore() throws Exception {
        // given
        final MessageStore messageStore = mock(MessageStore.class);
        final CompactedEventSource eventSource = new CompactedEventSource("foo", messageStore, mock(MessageLogReceiverEndpoint.class));

        // when
        eventSource.consume();

        // then
        verify(messageStore).close();
    }
}