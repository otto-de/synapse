package de.otto.synapse.configuration.aws;

import de.otto.synapse.aws.s3.SnapshotWriteService;
import de.otto.synapse.compaction.aws.CompactionService;
import de.otto.synapse.eventsource.EventSourceBuilder;
import de.otto.synapse.state.ConcurrentHashMapStateRepository;
import de.otto.synapse.state.StateRepository;
import de.otto.synapse.state.concurrent.ConcurrentHashMapConcurrentMapStateRepository;
import de.otto.synapse.state.concurrent.ConcurrentMapStateRepository;
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
    @ConditionalOnMissingBean(name = "compactionConcurrentStateRepository")
    public ConcurrentMapStateRepository<String> compactionConcurrentStateRepository() {
        return new ConcurrentHashMapConcurrentMapStateRepository<>();
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "synapse.compaction", name = "enabled", havingValue = "true")
    public CompactionService compactionService(final SnapshotWriteService snapshotWriteService,
                                               final ConcurrentMapStateRepository<String> compactionStateRepository,
                                               final EventSourceBuilder defaultEventSourceBuilder) {
        return new CompactionService(snapshotWriteService, compactionStateRepository, defaultEventSourceBuilder);
    }
}
