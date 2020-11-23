package de.otto.synapse.endpoint.receiver.kafka;

import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.Consumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.test.context.junit4.SpringRunner;

import java.time.Duration;
import java.util.concurrent.ExecutorService;

import static de.otto.synapse.channel.ChannelPosition.fromHorizon;
import static de.otto.synapse.channel.StopCondition.shutdown;
import static java.util.Collections.singletonList;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.mockito.internal.util.collections.Sets.newSet;

@RunWith(SpringRunner.class)
    public class KafkaMessageLogReceiverEndpointTest {

    public static final String KAFKA_TOPIC = "test-stream";

    private Consumer<String, String> kafkaConsumer;
    private KafkaMessageLogReceiverEndpoint endpoint;
    private MessageInterceptorRegistry interceptorRegistry = new MessageInterceptorRegistry();
    private ExecutorService executorService = newSingleThreadExecutor();

    @Before
    public void setUp() {
        final String groupId = "KafkaMessageLogReceiverEndpointTest";

        kafkaConsumer = mock(Consumer.class);
        endpoint = new KafkaMessageLogReceiverEndpoint(
                KAFKA_TOPIC,
                interceptorRegistry,
                kafkaConsumer,
                executorService,
                mock(ApplicationEventPublisher.class));
    }

    @After
    public void tearDown() {
        endpoint.stop();
    }

    @Test(expected = IllegalStateException.class)
    public void shouldFailToSubscribeToWrongKafkaConsumer() throws Exception {
        // given
        when(kafkaConsumer.subscription()).thenReturn(newSet("old-stream"));

        // when
        endpoint.consumeUntil(fromHorizon(), shutdown());
    }

    @Test
    public void shouldUnsubscribeIfKafkaConsumerIsAlreadySubscribed() throws Exception {
        // given
        when(kafkaConsumer.subscription()).thenReturn(newSet(KAFKA_TOPIC));

        // when
        endpoint.consumeUntil(fromHorizon(), shutdown());

        // then
        verify(kafkaConsumer).unsubscribe();
        verify(kafkaConsumer).subscribe(eq(singletonList(KAFKA_TOPIC)), any(ConsumerRebalanceListener.class));
    }

    @Test
    public void shouldHandleExceptionsDuringPoll() throws Exception {
        // given
        when(kafkaConsumer.subscription()).thenReturn(newSet(KAFKA_TOPIC));
        when(kafkaConsumer.poll(any(Duration.class))).thenThrow(RuntimeException.class);

        // when
        endpoint.consumeUntil(fromHorizon(), shutdown());

        // then
    }

}