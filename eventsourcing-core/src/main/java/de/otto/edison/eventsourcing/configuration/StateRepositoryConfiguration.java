package de.otto.edison.eventsourcing.configuration;

import de.otto.edison.eventsourcing.state.DefaultStateRepository;
import de.otto.edison.eventsourcing.state.StateRepository;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class StateRepositoryConfiguration {

    @Bean
    @ConditionalOnMissingBean(StateRepository.class)
    public StateRepository<String> stateRepository() {
        return new DefaultStateRepository<>();
    }

}
