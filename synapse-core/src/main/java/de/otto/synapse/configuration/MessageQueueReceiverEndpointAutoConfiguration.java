package de.otto.synapse.configuration;

import de.otto.synapse.annotation.messagequeue.MessageQueueConsumerBeanPostProcessor;
import de.otto.synapse.endpoint.receiver.MessageQueueConsumerProcess;
import de.otto.synapse.endpoint.receiver.MessageQueueReceiverEndpoint;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Role;

import java.util.List;

import static org.slf4j.LoggerFactory.getLogger;
import static org.springframework.beans.factory.config.BeanDefinition.ROLE_INFRASTRUCTURE;

@ImportAutoConfiguration(SynapseAutoConfiguration.class)
@EnableConfigurationProperties(ConsumerProcessProperties.class)
public class MessageQueueReceiverEndpointAutoConfiguration {

    private static final Logger LOG = getLogger(MessageQueueReceiverEndpointAutoConfiguration.class);

    @Autowired(required = false)
    private List<MessageQueueReceiverEndpoint> messageQueueReceiverEndpoints;

    @Bean
    @ConditionalOnProperty(
            prefix = "synapse",
            name = "consumer-process.enabled",
            havingValue = "true",
            matchIfMissing = true)
    public MessageQueueConsumerProcess messageQueueConsumerProcess() {
        return new MessageQueueConsumerProcess(messageQueueReceiverEndpoints);
    }

    @Bean
    @Role(ROLE_INFRASTRUCTURE)
    public MessageQueueConsumerBeanPostProcessor messageQueueConsumerAnnotationBeanPostProcessor() {
        return new MessageQueueConsumerBeanPostProcessor();
    }

}
