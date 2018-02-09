package de.otto.edison.eventsourcing.aws.configuration;

import de.otto.edison.eventsourcing.EventSourceBuilder;
import de.otto.edison.eventsourcing.aws.compaction.CompactionService;
import de.otto.edison.eventsourcing.aws.s3.SnapshotWriteService;
import de.otto.edison.eventsourcing.state.ConcurrentHashMapStateRepository;
import de.otto.edison.eventsourcing.state.StateRepository;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(CompactionProperties.class)
@ImportAutoConfiguration(SnapshotAutoConfiguration.class)
public class CompactionAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(name = "compactionStateRepository")
    public StateRepository<String> compactionStateRepository() {
        return new ConcurrentHashMapStateRepository<>();
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "edison.eventsourcing.compaction", name = "enabled", havingValue = "true")
    public CompactionService compactionService(final SnapshotWriteService snapshotWriteService,
                                               final StateRepository<String> compactionStateRepository,
                                               final EventSourceBuilder defaultEventSourceBuilder) {
        return new CompactionService(snapshotWriteService, compactionStateRepository, defaultEventSourceBuilder);
    }
}
