package de.otto.synapse.configuration;

import de.otto.synapse.annotation.MessageQueueConsumerBeanPostProcessor;
import de.otto.synapse.endpoint.receiver.MessageQueueConsumerProcess;
import de.otto.synapse.endpoint.receiver.MessageQueueReceiverEndpoint;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Role;

import java.util.List;

import static org.springframework.beans.factory.config.BeanDefinition.ROLE_INFRASTRUCTURE;

@Import(SynapseAutoConfiguration.class)
public class MessageQueueReceiverEndpointAutoConfiguration {

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
    public static MessageQueueConsumerBeanPostProcessor messageQueueConsumerAnnotationBeanPostProcessor() {
        return new MessageQueueConsumerBeanPostProcessor();
    }

}
