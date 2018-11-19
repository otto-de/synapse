package de.otto.synapse.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.annotation.MessageInterceptorBeanPostProcessor;
import de.otto.synapse.endpoint.DefaultSenderHeadersInterceptor;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import org.slf4j.Logger;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Role;

import java.util.Map;

import static com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS;
import static org.slf4j.LoggerFactory.getLogger;
import static org.springframework.beans.factory.config.BeanDefinition.ROLE_INFRASTRUCTURE;

@Configuration
@EnableConfigurationProperties(SynapseProperties.class)
public class SynapseAutoConfiguration {

    private static final Logger LOG = getLogger(SynapseAutoConfiguration.class);

    private MessageInterceptorRegistry registry;

    @Bean
    public ObjectMapper objectMapper() {
        final ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.findAndRegisterModules();
        objectMapper.disable(WRITE_DATES_AS_TIMESTAMPS);
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

    /**
     * Configures a {@link de.otto.synapse.endpoint.MessageInterceptor} that is used to add some default
     * message headers when messages are sent to a {@link de.otto.synapse.endpoint.sender.MessageSenderEndpoint}.
     *
     * @param synapseProperties properties used to configure the interceptor
     * @return DefaultSenderHeadersInterceptor
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(
            prefix = "synapse.sender.default-headers",
            name = "enabled",
            havingValue = "true",
            matchIfMissing = true)
    public DefaultSenderHeadersInterceptor defaultSenderHeadersInterceptor(final SynapseProperties synapseProperties) {
        return new DefaultSenderHeadersInterceptor(synapseProperties.getSender());
    }

    /**
     * Activate the MessageInterceptorBeanPostProcessor used to pose-process beans having methods annotated as a
     * {@link de.otto.synapse.annotation.MessageInterceptor}.
     *
     * @return MessageInterceptorBeanPostProcessor
     */
    @Bean
    @Role(ROLE_INFRASTRUCTURE)
    public MessageInterceptorBeanPostProcessor messageInterceptorBeanPostProcessor() {
        return new MessageInterceptorBeanPostProcessor();
    }

}
