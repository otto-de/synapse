package de.otto.synapse.eventsource;

import de.otto.synapse.channel.InMemoryChannel;
import de.otto.synapse.consumer.TestMessageConsumer;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.message.Key;
import de.otto.synapse.message.Message;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static de.otto.synapse.consumer.TestMessageConsumer.testEventConsumer;
import static de.otto.synapse.messagestore.MessageStores.emptyMessageStore;
import static java.util.Collections.singletonList;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.*;

public class EventSourceConsumerProcessTest {

    @Test
    public void shouldStopEventSources() {
        final EventSource eventSource = mock(EventSource.class);

        final EventSourceConsumerProcess process = new EventSourceConsumerProcess(singletonList(eventSource));
        process.start();
        process.stop();

        verify(eventSource).stop();
    }

    @Test
    public void shouldStopEventSourcesOnError() {
        final EventSource eventSource = mock(EventSource.class);
        when(eventSource.consume()).thenThrow(IllegalStateException.class);

        final EventSourceConsumerProcess process = new EventSourceConsumerProcess(singletonList(eventSource));
        process.start();

        await()
                .atMost(1, TimeUnit.SECONDS)
                .until(() -> !process.isRunning());

        verify(eventSource).stop();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldCallConsumeOnEventSource() throws InterruptedException {
        EventSource eventSource = mock(EventSource.class);

        EventSourceConsumerProcess process = new EventSourceConsumerProcess(singletonList(eventSource));
        process.start();
        process.stop();

        verify(eventSource, timeout(1000)).consume();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldInvokeTwoConsumersForSameEventSource() throws InterruptedException {
        final InMemoryChannel channel = new InMemoryChannel("test", new MessageInterceptorRegistry());
        final EventSource eventSource = new DefaultEventSource(emptyMessageStore(), channel);

        final TestMessageConsumer eventConsumerA = testEventConsumer(".*", String.class);
        final TestMessageConsumer eventConsumerB = testEventConsumer(".*", String.class);
        eventSource.register(eventConsumerA);
        eventSource.register(eventConsumerB);

        channel.send(Message.message(Key.of("test"), "some payload"));
        final EventSourceConsumerProcess process = new EventSourceConsumerProcess(singletonList(eventSource));
        process.start();
        process.stop();

        await()
                .atMost(1, TimeUnit.SECONDS)
                .until(() -> eventConsumerA.getConsumedMessages().size() == 1);
        await()
                .atMost(1, TimeUnit.SECONDS)
                .until(() -> eventConsumerB.getConsumedMessages().size() == 1);
    }

}
