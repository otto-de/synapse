package de.otto.synapse.example.consumer.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.annotation.EnableEventSource;
import de.otto.synapse.channel.InMemoryChannels;
import de.otto.synapse.configuration.MessageEndpointConfigurer;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.sender.InMemoryMessageSenderFactory;
import de.otto.synapse.endpoint.sender.MessageSenderEndpoint;
import de.otto.synapse.endpoint.sender.MessageSenderFactory;
import de.otto.synapse.eventsource.EventSourceBuilder;
import de.otto.synapse.eventsource.InMemoryEventSourceBuilder;
import de.otto.synapse.example.consumer.state.BananaProduct;
import de.otto.synapse.state.ConcurrentHashMapStateRepository;
import de.otto.synapse.state.StateRepository;
import org.slf4j.Logger;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static de.otto.synapse.endpoint.MessageInterceptorRegistration.receiverChannelsWith;
import static de.otto.synapse.endpoint.MessageInterceptorRegistration.senderChannelsWith;
import static org.slf4j.LoggerFactory.getLogger;


@Configuration
@EnableConfigurationProperties({MyServiceProperties.class})
@EnableEventSource(name = "bananaSource",  channelName = "${exampleservice.banana-channel}")
@EnableEventSource(name = "productSource", channelName = "${exampleservice.product-channel}")
public class ExampleConfiguration implements MessageEndpointConfigurer {

    private static final Logger LOG = getLogger(ExampleConfiguration.class);

    @Override
    public void configureMessageInterceptors(final MessageInterceptorRegistry registry) {
        registry.register(receiverChannelsWith((m) -> {
            LOG.info("[receiver] Intercepted message " + m);
            return m;
        }));
        registry.register(senderChannelsWith((m) -> {
            LOG.info("[sender] Intercepted message " + m);
            return m;
        }));
    }

    @Bean
    public StateRepository<BananaProduct> bananaProductConcurrentStateRepository() {
        return new ConcurrentHashMapStateRepository<>();
    }

    @Bean
    public InMemoryChannels inMemoryChannels(final ObjectMapper objectMapper, final ApplicationEventPublisher eventPublisher) {
        return new InMemoryChannels(objectMapper, eventPublisher);
    }

    @Bean
    public MessageSenderFactory messageSenderFactory(final MessageInterceptorRegistry registry,
                                                     final ObjectMapper objectMapper,
                                                     final InMemoryChannels inMemoryChannels) {
        return new InMemoryMessageSenderFactory(registry, inMemoryChannels, objectMapper);
    }


    @Bean
    public EventSourceBuilder defaultEventSourceBuilder(final MessageInterceptorRegistry interceptorRegistry,
                                                        final InMemoryChannels inMemoryChannels) {
        return new InMemoryEventSourceBuilder(interceptorRegistry, inMemoryChannels);
    }

    @Bean
    public MessageSenderEndpoint bananaMessageSender(final MessageSenderFactory messageSenderFactory,
                                                     final MyServiceProperties properties) {
        return messageSenderFactory.createSenderFor(properties.getBananaChannel());
    }

    @Bean
    public MessageSenderEndpoint productMessageSender(final MessageSenderFactory messageSenderFactory,
                                                      final MyServiceProperties properties) {
        return messageSenderFactory.createSenderFor(properties.getProductChannel());
    }

}
