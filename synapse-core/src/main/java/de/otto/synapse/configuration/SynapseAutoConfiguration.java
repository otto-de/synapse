package de.otto.synapse.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import de.otto.synapse.annotation.EventSourceConsumerBeanPostProcessor;
import de.otto.synapse.annotation.messagequeue.MessageQueueConsumerBeanPostProcessor;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.receiver.MessageQueueConsumerProcess;
import de.otto.synapse.endpoint.receiver.MessageQueueReceiverEndpoint;
import de.otto.synapse.eventsource.DefaultEventSource;
import de.otto.synapse.eventsource.EventSource;
import de.otto.synapse.eventsource.EventSourceBuilder;
import de.otto.synapse.eventsource.EventSourceConsumerProcess;
import de.otto.synapse.messagestore.MessageStoreFactory;
import de.otto.synapse.messagestore.SnapshotMessageStore;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Role;

import java.util.List;
import java.util.Map;

import static org.slf4j.LoggerFactory.getLogger;
import static org.springframework.beans.factory.config.BeanDefinition.ROLE_INFRASTRUCTURE;

@Configuration
@EnableConfigurationProperties(ConsumerProcessProperties.class)
public class SynapseAutoConfiguration {

    private static final Logger LOG = getLogger(SynapseAutoConfiguration.class);

    private MessageInterceptorRegistry registry;

    @Bean
    @ConditionalOnMissingBean
    public ObjectMapper objectMapper() {
        final ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.findAndRegisterModules();
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        return objectMapper;
    }

    @Bean
    public MessageInterceptorRegistry messageInterceptorRegistry(final ApplicationContext applicationContext) {
        if (registry == null) {
            this.registry = new MessageInterceptorRegistry();
            final Map<String, MessageEndpointConfigurer> configurers = applicationContext.getBeansOfType(MessageEndpointConfigurer.class);
            if (configurers != null) {
                configurers.forEach((beanName, bean) -> {
                    LOG.info("Configuring MessageEndpointConfigurer '" + beanName + "'");
                    bean.configureMessageInterceptors(registry);
                });
            }
        }
        return registry;
    }
}
