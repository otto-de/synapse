package de.otto.edison.eventsourcing.example.producer.configuration;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;


@Configuration
@EnableConfigurationProperties({MyServiceProperties.class})
public class ExampleConfiguration {


}
