package de.otto.edison.eventsourcing.example.producer.configuration;

import de.otto.edison.eventsourcing.EventSender;
import de.otto.edison.eventsourcing.EventSenderFactory;
import de.otto.edison.eventsourcing.example.producer.configuration.MyServiceProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(MyServiceProperties.class)
public class EventSenderConfiguration {

    @Bean
    public EventSender productEventSender(final EventSenderFactory eventSenderFactory,
                                          final MyServiceProperties properties) {
        return eventSenderFactory.createSenderForStream(properties.getProductStreamName());
    }
}
