package de.otto.synapse.configuration;

import de.otto.synapse.annotation.MessageLogConsumerBeanPostProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Role;

import static org.springframework.beans.factory.config.BeanDefinition.ROLE_INFRASTRUCTURE;

@Import(SynapseAutoConfiguration.class)
public class MessageLogReceiverEndpointAutoConfiguration {

    @Bean
    @Role(ROLE_INFRASTRUCTURE)
    public MessageLogConsumerBeanPostProcessor messageLogConsumerAnnotationBeanPostProcessor() {
        return new MessageLogConsumerBeanPostProcessor();
    }

}
