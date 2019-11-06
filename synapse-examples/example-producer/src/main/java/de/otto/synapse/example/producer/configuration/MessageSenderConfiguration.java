package de.otto.synapse.example.producer.configuration;

import de.otto.synapse.configuration.InMemoryMessageLogTestConfiguration;
import de.otto.synapse.configuration.MessageEndpointConfigurer;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.sender.MessageSenderEndpoint;
import de.otto.synapse.endpoint.sender.MessageSenderEndpointFactory;
import de.otto.synapse.translator.MessageFormat;
import org.slf4j.Logger;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.annotation.EnableScheduling;

import static de.otto.synapse.endpoint.MessageInterceptorRegistration.receiverChannelsWith;
import static org.slf4j.LoggerFactory.getLogger;

@Configuration
@Import(InMemoryMessageLogTestConfiguration.class)
@EnableConfigurationProperties(MyServiceProperties.class)
@EnableScheduling
public class MessageSenderConfiguration implements MessageEndpointConfigurer {

    private static final Logger LOG = getLogger(MessageSenderConfiguration.class);

    @Override
    public void configureMessageInterceptors(final MessageInterceptorRegistry registry) {
        registry.register(receiverChannelsWith((message) -> {
            LOG.info("Sending message {}", message);
            return message;
        }));
    }

    @Bean
    public MessageSenderEndpoint productMessageSender(final MessageSenderEndpointFactory messageLogSenderEndpointFactory,
                                                      final MyServiceProperties properties) {
        return messageLogSenderEndpointFactory.create(properties.getProductChannelName(), MessageFormat.V1);

    }
}
