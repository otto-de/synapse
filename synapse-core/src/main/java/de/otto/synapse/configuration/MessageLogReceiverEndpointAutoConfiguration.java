package de.otto.synapse.configuration;

import de.otto.synapse.annotation.MessageLogConsumerBeanPostProcessor;
import de.otto.synapse.endpoint.receiver.MessageLogConsumerContainer;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpoint;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Role;

import java.util.List;

import static org.slf4j.LoggerFactory.getLogger;
import static org.springframework.beans.factory.config.BeanDefinition.ROLE_INFRASTRUCTURE;

@Import(SynapseAutoConfiguration.class)
@EnableConfigurationProperties(SynapseProperties.class)
public class MessageLogReceiverEndpointAutoConfiguration {

    private static final Logger LOG = getLogger(MessageLogReceiverEndpointAutoConfiguration.class);

    @Bean
    @Role(ROLE_INFRASTRUCTURE)
    public MessageLogConsumerBeanPostProcessor messageLogConsumerAnnotationBeanPostProcessor() {
        return new MessageLogConsumerBeanPostProcessor();
    }

}
