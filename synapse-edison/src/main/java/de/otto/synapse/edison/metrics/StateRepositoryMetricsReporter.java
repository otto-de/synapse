package de.otto.synapse.edison.metrics;


import de.otto.synapse.state.StateRepository;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.List;

@ConditionalOnProperty(
        prefix = "synapse.edison.metrics",
        name = "enabled",
        havingValue = "true")
@Component
public class StateRepositoryMetricsReporter {

    public StateRepositoryMetricsReporter(MeterRegistry registry,
                                          @Value("${spring.profiles.active}") String profile,
                                          List<StateRepository<?>> stateRepositories) {
        stateRepositories.forEach(stateRepository -> {
            Gauge.builder("state_repository_size", stateRepository, StateRepository::size)
                    .tag("profile", profile)
                    .tag("state_repository", stateRepository.getName())
                    .register(registry);
        });
    }

}
