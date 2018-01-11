package de.otto.edison.eventsourcing.example.consumer.configuration;

import de.otto.edison.eventsourcing.annotation.EnableEventSource;
import de.otto.edison.eventsourcing.example.consumer.state.BananaProduct;
import de.otto.edison.eventsourcing.state.DefaultStateRepository;
import de.otto.edison.eventsourcing.state.StateRepository;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties({MyServiceProperties.class})
@EnableEventSource(name = "bananaSource",  streamName = "${exampleservice.banana-stream-name}")
@EnableEventSource(name = "productSource", streamName = "${exampleservice.product-stream-name}")
public class ExampleConfiguration {

    @Bean
    public StateRepository<BananaProduct> bananaProductStateRepository() {
        return new DefaultStateRepository<>();
    }

}
