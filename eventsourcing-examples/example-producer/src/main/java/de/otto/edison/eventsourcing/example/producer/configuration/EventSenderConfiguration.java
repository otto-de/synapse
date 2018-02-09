package de.otto.edison.eventsourcing.example.producer.configuration;

import de.otto.edison.eventsourcing.MessageSender;
import de.otto.edison.eventsourcing.MessageSenderFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(MyServiceProperties.class)
public class EventSenderConfiguration {

    @Bean
    public MessageSender productEventSender(final MessageSenderFactory messageSenderFactory,
                                            final MyServiceProperties properties) {
        return messageSenderFactory.createSenderForStream(properties.getProductStreamName());
    }
}
