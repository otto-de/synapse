package de.otto.synapse.configuration.kafka;

import de.otto.synapse.configuration.SynapseAutoConfiguration;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.sender.MessageSenderEndpointFactory;
import de.otto.synapse.endpoint.sender.kafka.KafkaMessageSenderEndpointFactory;
import org.slf4j.Logger;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.core.KafkaTemplate;

import static org.slf4j.LoggerFactory.getLogger;

@Configuration
@Import(SynapseAutoConfiguration.class)
@EnableKafka
@EnableKafkaStreams
public class KafkaAutoConfiguration {

    private static final Logger LOG = getLogger(KafkaAutoConfiguration.class);

    @Bean
    @ConditionalOnMissingBean(name = "kafkaMessageLogSenderEndpointFactory")
    public MessageSenderEndpointFactory kafkaMessageLogSenderEndpointFactory(final MessageInterceptorRegistry registry,
                                                                             final KafkaTemplate<String, String> kafkaTemplate) {
        LOG.info("Auto-configuring Kafka MessageSenderEndpointFactory");
        return new KafkaMessageSenderEndpointFactory(registry, kafkaTemplate);
    }

}
