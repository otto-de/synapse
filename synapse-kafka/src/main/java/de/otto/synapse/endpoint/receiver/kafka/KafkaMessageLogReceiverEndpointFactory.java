package de.otto.synapse.endpoint.receiver.kafka;

import de.otto.synapse.channel.selector.Kafka;
import de.otto.synapse.channel.selector.Selector;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpoint;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpointFactory;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.context.ApplicationEventPublisher;

import javax.annotation.Nonnull;
import java.util.concurrent.ExecutorService;

public class KafkaMessageLogReceiverEndpointFactory implements MessageLogReceiverEndpointFactory {

    private final MessageInterceptorRegistry interceptorRegistry;
    private final KafkaConsumer<String, String> kafkaConsumer;
    private final ApplicationEventPublisher eventPublisher;
    private final ExecutorService executorService;

    public KafkaMessageLogReceiverEndpointFactory(final MessageInterceptorRegistry interceptorRegistry,
                                                  final KafkaConsumer<String, String> kafkaConsumer,
                                                  final ExecutorService kinesisMessageLogExecutorService,
                                                  final ApplicationEventPublisher eventPublisher) {
        this.interceptorRegistry = interceptorRegistry;
        this.kafkaConsumer = kafkaConsumer;
        this.executorService = kinesisMessageLogExecutorService;
        this.eventPublisher = eventPublisher;
    }


    @Override
    public MessageLogReceiverEndpoint create(@Nonnull String channelName) {
        return new KafkaMessageLogReceiverEndpoint(
                channelName,
                interceptorRegistry,
                kafkaConsumer,
                executorService,
                eventPublisher);
    }

    @Override
    public boolean matches(Class<? extends Selector> channelSelector) {
        return channelSelector.isAssignableFrom(selector());
    }

    @Override
    public Class<? extends Selector> selector() {
        return Kafka.class;
    }

}
