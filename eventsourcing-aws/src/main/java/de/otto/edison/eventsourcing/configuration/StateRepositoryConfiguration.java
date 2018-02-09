package de.otto.edison.eventsourcing.configuration;

import de.otto.edison.eventsourcing.state.DefaultStateRepository;
import de.otto.edison.eventsourcing.state.StateRepository;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class StateRepositoryConfiguration {

    @Bean(name = "compactionStateRepository")
    @ConditionalOnMissingBean(name = "compactionStateRepository")
    public StateRepository<String> compactionStateRepository() {
        return new DefaultStateRepository<>();
    }

}
