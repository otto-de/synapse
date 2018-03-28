package de.otto.synapse.example.producer.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.channel.InMemoryChannels;
import de.otto.synapse.endpoint.MessageInterceptor;
import de.otto.synapse.endpoint.sender.InMemoryMessageSender;
import de.otto.synapse.endpoint.sender.MessageSenderEndpoint;
import de.otto.synapse.endpoint.sender.MessageSenderFactory;
import de.otto.synapse.translator.JsonStringMessageTranslator;
import de.otto.synapse.translator.MessageTranslator;
import org.slf4j.Logger;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.slf4j.LoggerFactory.getLogger;

@Configuration
@EnableConfigurationProperties(MyServiceProperties.class)
public class MessageSenderConfiguration {

    private static final Logger LOG = getLogger(MessageSenderConfiguration.class);

    public static final MessageInterceptor LOGGING_INTERCEPTOR = (message) -> {
        LOG.info("Sending message {}", message);
        return message;
    };

    @Bean
    public InMemoryChannels inMemoryChannels(final ObjectMapper objectMapper) {
        return new InMemoryChannels(objectMapper);
    }

    @Bean
    public MessageSenderFactory messageSenderFactory(final InMemoryChannels inMemoryChannels,
                                                     final ObjectMapper objectMapper) {
        final MessageTranslator<String> messageTranslator = new JsonStringMessageTranslator(objectMapper);
        return channelName -> {
            return new InMemoryMessageSender(
                    messageTranslator,
                    inMemoryChannels.getChannel(channelName),
                    LOGGING_INTERCEPTOR);
        };
    }

    @Bean
    public MessageSenderEndpoint productMessageSender(final MessageSenderFactory messageSenderFactory,
                                                      final MyServiceProperties properties) {
        return messageSenderFactory.createSenderForStream(properties.getProductChannelName());

    }
}
