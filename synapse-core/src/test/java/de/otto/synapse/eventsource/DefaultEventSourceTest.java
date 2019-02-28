package de.otto.synapse.eventsource;

import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.consumer.MessageDispatcher;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpoint;
import de.otto.synapse.message.Header;
import de.otto.synapse.message.Key;
import de.otto.synapse.message.TextMessage;
import de.otto.synapse.messagestore.MessageStore;
import de.otto.synapse.messagestore.MessageStoreEntry;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static de.otto.synapse.channel.ChannelPosition.channelPosition;
import static de.otto.synapse.channel.ChannelPosition.fromHorizon;
import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static de.otto.synapse.channel.StopCondition.shutdown;
import static de.otto.synapse.messagestore.MessageStores.emptyMessageStore;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class DefaultEventSourceTest {

    @Test
    public void shouldReadMessagesFromMessageStore() throws ExecutionException, InterruptedException {
        // given
        final MessageStore messageStore = mockMessageStore(fromHorizon());
        final MessageLogReceiverEndpoint messageLog = mockMessageLogReceiverEndpoint();
        final DefaultEventSource eventSource = new DefaultEventSource(messageStore, messageLog);

        // when
        eventSource.consume().get();

        // then
        verify(messageStore).stream();
    }

    @Test
    public void shouldReadMessagesFromMessageLog() throws ExecutionException, InterruptedException {
        // given
        final MessageStore messageStore = mockMessageStore(fromHorizon());
        final MessageLogReceiverEndpoint messageLog = mockMessageLogReceiverEndpoint();
        final DefaultEventSource eventSource = new DefaultEventSource(messageStore, messageLog);

        // when
        eventSource.consume().get();

        // then
        verify(messageLog).consumeUntil(fromHorizon(), shutdown());
    }

    @Test
    public void shouldInterceptMessagesFromMessageStore() throws ExecutionException, InterruptedException {
        // given
        // and some message store having a single message
        final MessageStore messageStore = mock(MessageStore.class);
        when(messageStore.stream()).thenReturn(Stream.of(
                MessageStoreEntry.of("some-channel", TextMessage.of(Key.of("1"), null)))
        );
        when(messageStore.getLatestChannelPosition("some-channel")).thenReturn(fromHorizon());
        // and some MessageLogReceiverEndpoint with an InterceptorChain:
        final MessageLogReceiverEndpoint messageLog = mock(MessageLogReceiverEndpoint.class);
        when(messageLog.getChannelName()).thenReturn("some-channel");
        when(messageLog.intercept(any(TextMessage.class))).thenReturn(null);
        when(messageLog.consumeUntil(any(ChannelPosition.class), any(Predicate.class))).thenReturn(completedFuture(fromHorizon()));

        // and our famous DefaultEventSource:
        final DefaultEventSource eventSource = new DefaultEventSource(messageStore, messageLog);

        // when
        eventSource.consume().get();

        // then
        verify(messageLog).intercept(
                TextMessage.of(Key.of("1"), null)
        );
    }

    @Test
    public void shouldDropNullMessagesInterceptedFromMessageStore() throws ExecutionException, InterruptedException {
        // given
        // and some message store having a single message
        final MessageStore messageStore = mock(MessageStore.class);
        when(messageStore.stream()).thenReturn(Stream.of(
                MessageStoreEntry.of("some-channel", TextMessage.of(Key.of("1"), Header.of(), null))));
        when(messageStore.getLatestChannelPosition("some-channel")).thenReturn(fromHorizon());
        // and some MessageLogReceiverEndpoint with our InterceptorChain:
        final MessageLogReceiverEndpoint messageLog = mock(MessageLogReceiverEndpoint.class);
        when(messageLog.getChannelName()).thenReturn("some-channel");
        when(messageLog.consumeUntil(any(ChannelPosition.class), any(Predicate.class))).thenReturn(completedFuture(fromHorizon()));
        when(messageLog.intercept(any(TextMessage.class))).thenReturn(null);
        final MessageDispatcher messageDispatcher = mock(MessageDispatcher.class);
        when(messageLog.getMessageDispatcher()).thenReturn(messageDispatcher);
        // and our famous DefaultEventSource:
        final DefaultEventSource eventSource = new DefaultEventSource(messageStore, messageLog);

        // when
        eventSource.consume().get();

        // then
        verify(messageDispatcher, never()).accept(any(TextMessage.class));
    }

    @Test
    public void shouldDispatchMessagesFromMessageStore() throws ExecutionException, InterruptedException {
        // given
        // and some message store having a single message
        final MessageStore messageStore = mock(MessageStore.class);
        when(messageStore.stream()).thenReturn(Stream.of(
                MessageStoreEntry.of("some-channel", TextMessage.of(Key.of("1"), null))));
        when(messageStore.getLatestChannelPosition(anyString())).thenReturn(fromHorizon());

        // and some MessageLogReceiverEndpoint with our InterceptorChain:
        final MessageLogReceiverEndpoint messageLog = mock(MessageLogReceiverEndpoint.class);
        when(messageLog.getChannelName()).thenReturn("some-channel");
        when(messageLog.intercept(any(TextMessage.class))).thenReturn(TextMessage.of(Key.of("1"), null));
        when(messageLog.consumeUntil(any(ChannelPosition.class), any(Predicate.class))).thenReturn(completedFuture(fromHorizon()));
        final MessageDispatcher messageDispatcher = mock(MessageDispatcher.class);
        when(messageLog.getMessageDispatcher()).thenReturn(messageDispatcher);

        // and our famous DefaultEventSource:
        final DefaultEventSource eventSource = new DefaultEventSource(messageStore, messageLog);

        // when
        eventSource.consume().get();

        // then
        verify(messageDispatcher).accept(TextMessage.of(Key.of("1"), null));
    }

    @Test
    public void shouldContinueWithChannelPositionFromMessageStore() throws ExecutionException, InterruptedException {
        // given
        final ChannelPosition expectedChannelPosition = channelPosition(fromPosition("bar", "42"));
        // and some message store having a single message
        final MessageStore messageStore = mockMessageStore(expectedChannelPosition);
        // and some MessageLogReceiverEndpoint with our InterceptorChain:
        final MessageLogReceiverEndpoint messageLog = mockMessageLogReceiverEndpoint();
        // and our famous DefaultEventSource:
        final DefaultEventSource eventSource = new DefaultEventSource(messageStore, messageLog);

        // when
        eventSource.consume().get();

        // then
        verify(messageLog).consumeUntil(expectedChannelPosition, shutdown());
    }

    @Test
    public void shouldReturnChannelPositionFromMessageLog() throws ExecutionException, InterruptedException {
        // given
        final ChannelPosition messageStoreChannelPosition = channelPosition(fromPosition("bar", "42"));
        final ChannelPosition expectedChannelPosition = channelPosition(fromPosition("bar", "4711"));
        // and some message store having a single message
        final MessageStore messageStore = mockMessageStore(messageStoreChannelPosition);
        // and some MessageLogReceiverEndpoint with our InterceptorChain:
        final MessageLogReceiverEndpoint messageLog = mockMessageLogReceiverEndpoint(expectedChannelPosition);
        // and our famous DefaultEventSource:
        final DefaultEventSource eventSource = new DefaultEventSource(messageStore, messageLog);

        // when
        final ChannelPosition finalChannelPosition = eventSource.consume().get();

        // then
        verify(messageLog).consumeUntil(messageStoreChannelPosition, shutdown());
        assertThat(finalChannelPosition, is(expectedChannelPosition));
    }

    @Test
    public void shouldCloseMessageStore() throws Exception {
        // given
        final MessageStore messageStore = mockMessageStore(fromHorizon());
        final MessageLogReceiverEndpoint messageLog = mockMessageLogReceiverEndpoint();
        final DefaultEventSource eventSource = new DefaultEventSource(messageStore, messageLog);

        // when
        eventSource.consume().get();

        // then
        verify(messageStore).close();
    }

    @Test
    public void shouldStopMessageLogReceiverEndpoint() {
        // given
        final MessageLogReceiverEndpoint messageLog = mock(MessageLogReceiverEndpoint.class);
        when(messageLog.getChannelName()).thenReturn("test-channel");
        final DefaultEventSource eventSource = new DefaultEventSource(emptyMessageStore(), messageLog);

        // when
        eventSource.stop();

        // then
        verify(messageLog).stop();
        assertThat(eventSource.isStopping(), is(true));
    }

    private MessageLogReceiverEndpoint mockMessageLogReceiverEndpoint() {
        final MessageLogReceiverEndpoint messageLog = mock(MessageLogReceiverEndpoint.class);
        when(messageLog.getChannelName()).thenReturn("some-channel");
        when(messageLog.consumeUntil(any(ChannelPosition.class), any(Predicate.class))).thenReturn(completedFuture(fromHorizon()));
        return messageLog;
    }

    private MessageLogReceiverEndpoint mockMessageLogReceiverEndpoint(final ChannelPosition channelPosition) {
        final MessageLogReceiverEndpoint messageLog = mock(MessageLogReceiverEndpoint.class);
        when(messageLog.getChannelName()).thenReturn("some-channel");
        when(messageLog.consumeUntil(any(ChannelPosition.class), any(Predicate.class))).thenReturn(completedFuture(channelPosition));
        return messageLog;
    }

    private MessageStore mockMessageStore(final ChannelPosition expectedChannelPosition) {
        final MessageStore messageStore = mock(MessageStore.class);
        when(messageStore.getLatestChannelPosition("some-channel")).thenReturn(expectedChannelPosition);
        when(messageStore.stream()).thenReturn(Stream.empty());
        return messageStore;
    }
}