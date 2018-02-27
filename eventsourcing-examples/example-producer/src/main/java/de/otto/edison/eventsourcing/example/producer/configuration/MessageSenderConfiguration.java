package de.otto.edison.eventsourcing.example.producer.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.eventsourcing.MessageSender;
import de.otto.edison.eventsourcing.MessageSenderFactory;
import de.otto.edison.eventsourcing.inmemory.InMemoryMessageSender;
import de.otto.edison.eventsourcing.inmemory.InMemoryStream;
import de.otto.edison.eventsourcing.message.Message;
import de.otto.edison.eventsourcing.translator.JsonStringMessageTranslator;
import de.otto.edison.eventsourcing.translator.MessageTranslator;
import org.slf4j.Logger;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static de.otto.edison.eventsourcing.inmemory.InMemoryStreams.getChannel;
import static org.slf4j.LoggerFactory.getLogger;

@Configuration
@EnableConfigurationProperties(MyServiceProperties.class)
public class MessageSenderConfiguration {

    private static final Logger LOG = getLogger(MessageSenderConfiguration.class);

    @Bean
    public MessageSenderFactory messageSenderFactory(final ObjectMapper objectMapper) {
        final MessageTranslator<String> messageTranslator = new JsonStringMessageTranslator(objectMapper);
        return streamName -> {
            final InMemoryStream channel = getChannel(streamName);
            return new MessageSender() {
                final MessageSender delegate = new InMemoryMessageSender(messageTranslator, channel);

                @Override
                public <T> void send(Message<T> message) {
                    LOG.info("Sending message " + message);
                    delegate.send(message);
                }
            };
        };
    }

    @Bean
    public MessageSender productMessageSender(final MessageSenderFactory messageSenderFactory,
                                              final MyServiceProperties properties) {
        return messageSenderFactory.createSenderForStream(properties.getProductStreamName());

    }
}
