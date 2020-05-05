package de.otto.synapse.configuration.kafka;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import de.otto.synapse.configuration.SynapseAutoConfiguration;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpointFactory;
import de.otto.synapse.endpoint.receiver.kafka.KafkaMessageLogReceiverEndpointFactory;
import de.otto.synapse.endpoint.sender.MessageSenderEndpointFactory;
import de.otto.synapse.endpoint.sender.kafka.KafkaMessageSenderEndpointFactory;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.concurrent.ExecutorService;

import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.slf4j.LoggerFactory.getLogger;

@Configuration
@Import({
        SynapseAutoConfiguration.class,
        KafkaAutoConfiguration.class})
@EnableScheduling
@EnableKafka
@EnableConfigurationProperties(KafkaProperties.class)
public class SynapseKafkaAutoConfiguration {

    private static final Logger LOG = getLogger(SynapseKafkaAutoConfiguration.class);

    @Bean
    @ConditionalOnMissingBean(name = "kafkaMessageLogSenderEndpointFactory")
    public MessageSenderEndpointFactory kafkaMessageLogSenderEndpointFactory(final MessageInterceptorRegistry registry,
                                                                             final KafkaTemplate<String, String> kafkaTemplate) {
        LOG.info("Auto-configuring Kafka MessageSenderEndpointFactory");
        return new KafkaMessageSenderEndpointFactory(registry, kafkaTemplate);
    }

    @Bean
    @ConditionalOnMissingBean(name = "kafkaMessageLogReceiverEndpointFactory")
    public MessageLogReceiverEndpointFactory kafkaMessageLogReceiverEndpointFactory(final KafkaProperties kafkaProperties,
                                                                                    final MessageInterceptorRegistry interceptorRegistry,
                                                                                    final ApplicationEventPublisher eventPublisher) {
        LOG.info("Auto-configuring Kafka MessageLogReceiverEndpointFactory");
        final ExecutorService executorService = newCachedThreadPool(
                new ThreadFactoryBuilder().setNameFormat("kafka-message-log-%d").build()
        );

        final ConsumerFactory<String, String> consumerFactory = new DefaultKafkaConsumerFactory<>(
                kafkaProperties.buildConsumerProperties(),
                new StringDeserializer(),
                new StringDeserializer());
        final KafkaConsumer<String, String> kafkaConsumer = (KafkaConsumer<String, String>)consumerFactory.createConsumer();

        return new KafkaMessageLogReceiverEndpointFactory(
                interceptorRegistry,
                kafkaConsumer,
                executorService,
                eventPublisher);
    }

}
