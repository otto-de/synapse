package de.otto.synapse.example.producer.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.channel.InMemoryChannel;
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

import static de.otto.synapse.channel.InMemoryChannels.getChannel;
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
    public MessageSenderFactory messageSenderFactory(final ObjectMapper objectMapper) {
        final MessageTranslator<String> messageTranslator = new JsonStringMessageTranslator(objectMapper);
        return channelName -> {
            final InMemoryChannel channel = getChannel(channelName);
            return new InMemoryMessageSender(
                    messageTranslator,
                    channel,
                    LOGGING_INTERCEPTOR);
        };
    }

    @Bean
    public MessageSenderEndpoint productMessageSender(final MessageSenderFactory messageSenderFactory,
                                                      final MyServiceProperties properties) {
        return messageSenderFactory.createSenderForStream(properties.getProductStreamName());

    }
}
