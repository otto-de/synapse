package de.otto.synapse.example.producer.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.channel.InMemoryChannels;
import de.otto.synapse.configuration.MessageEndpointConfigurer;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.sender.InMemoryMessageSenderFactory;
import de.otto.synapse.endpoint.sender.MessageSenderEndpoint;
import de.otto.synapse.endpoint.sender.MessageSenderFactory;
import org.slf4j.Logger;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static de.otto.synapse.endpoint.MessageInterceptorRegistration.receiverChannelsWith;
import static org.slf4j.LoggerFactory.getLogger;

@Configuration
@EnableConfigurationProperties(MyServiceProperties.class)
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
    public InMemoryChannels inMemoryChannels(final ObjectMapper objectMapper, final ApplicationEventPublisher eventPublisher) {
        return new InMemoryChannels(objectMapper, eventPublisher);
    }

    @Bean
    public MessageSenderFactory messageSenderFactory(final MessageInterceptorRegistry registry,
                                                     final InMemoryChannels inMemoryChannels,
                                                     final ObjectMapper objectMapper) {
        return new InMemoryMessageSenderFactory(registry, inMemoryChannels, objectMapper);
    }

    @Bean
    public MessageSenderEndpoint productMessageSender(final MessageSenderFactory messageSenderFactory,
                                                      final MyServiceProperties properties) {
        return messageSenderFactory.createSenderFor(properties.getProductChannelName());

    }
}
