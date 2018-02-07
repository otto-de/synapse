package de.otto.edison.eventsourcing.statusdetail;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;

@Configuration
public class ClockConfiguration {

    @ConditionalOnMissingBean
    @Bean
    public Clock clock() {
        return Clock.systemDefaultZone();
    }
}
