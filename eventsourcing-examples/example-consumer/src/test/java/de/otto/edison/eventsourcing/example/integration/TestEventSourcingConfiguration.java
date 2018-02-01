package de.otto.edison.eventsourcing.example.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.eventsourcing.EventSenderFactory;
import de.otto.edison.eventsourcing.EventSourceBuilder;
import de.otto.edison.eventsourcing.example.consumer.configuration.MyServiceProperties;
import de.otto.edison.eventsourcing.inmemory.InMemoryEventSender;
import de.otto.edison.eventsourcing.inmemory.InMemoryEventSource;
import de.otto.edison.eventsourcing.inmemory.InMemoryStream;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(MyServiceProperties.class)
public class TestEventSourcingConfiguration {

    @Bean
    public InMemoryStream productStream() {
        return new InMemoryStream();
    }

    @Bean
    public InMemoryStream bananaStream() {
        return new InMemoryStream();
    }

    @Bean
    public EventSenderFactory eventSenderFactory(final ObjectMapper objectMapper,
                                                 final InMemoryStream productStream,
                                                 final InMemoryStream bananaStream,
                                                 final MyServiceProperties myServiceProperties) {
        return streamName -> {
            if (streamName.equals(myServiceProperties.getBananaStreamName())) {
                return new InMemoryEventSender(objectMapper, bananaStream);
            } else if (streamName.equals(myServiceProperties.getProductStreamName())) {
                return new InMemoryEventSender(objectMapper, productStream);
            } else {
                throw new IllegalArgumentException("no stream for name " + streamName + " available.");
            }
        };
    }


    @Bean
    public EventSourceBuilder defaultEventSourceBuilder(final InMemoryStream productStream,
                                                        final InMemoryStream bananaStream,
                                                        final MyServiceProperties myServiceProperties,
                                                        final ObjectMapper objectMapper) {
        return (name, streamName) -> {
            if (streamName.equals(myServiceProperties.getBananaStreamName())) {
                return new InMemoryEventSource(name, bananaStream, objectMapper);
            } else if (streamName.equals(myServiceProperties.getProductStreamName())) {
                return new InMemoryEventSource(name, productStream, objectMapper);
            } else {
                throw new IllegalArgumentException("no stream for name " + streamName + " available.");
            }
        };
    }

}
