package de.otto.synapse.edison.state;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(EdisonStateRepositoryUiProperties.class)
public class EdisonStateRepositoryUiAutoConfiguration {
}
