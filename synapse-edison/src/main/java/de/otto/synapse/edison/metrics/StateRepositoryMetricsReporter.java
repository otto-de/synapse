package de.otto.synapse.edison.metrics;


import de.otto.synapse.state.StateRepository;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.util.List;

@ConditionalOnProperty(
        prefix = "synapse.edison.metrics",
        name = "enabled",
        havingValue = "true")
@Component
public class StateRepositoryMetricsReporter {

    public StateRepositoryMetricsReporter(MeterRegistry registry,
                                          Environment environment,
                                          List<StateRepository<?>> stateRepositories) {
        stateRepositories.forEach(stateRepository -> {
            Gauge.builder("state_repository_size", stateRepository, StateRepository::size)
                    .tag("profile", String.join(",", environment.getActiveProfiles()))
                    .tag("state_repository", stateRepository.getName())
                    .register(registry);
        });
    }

}
