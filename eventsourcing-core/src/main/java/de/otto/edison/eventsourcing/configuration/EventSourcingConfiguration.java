package de.otto.edison.eventsourcing.configuration;

import de.otto.edison.eventsourcing.EventSourcingProperties;
import de.otto.edison.eventsourcing.consumer.EventConsumer;
import de.otto.edison.eventsourcing.consumer.EventSource;
import de.otto.edison.eventsourcing.consumer.EventSourceConsumerProcess;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

import static java.util.Collections.emptyList;

@Configuration
@EnableConfigurationProperties(EventSourcingProperties.class)
@ImportAutoConfiguration({
        EventSourcingBootstrapConfiguration.class,
        SnapshotConfiguration.class,
        KinesisConfiguration.class,
})
public class EventSourcingConfiguration {

    @Autowired(required = false)
    private List<EventConsumer> eventConsumers;
    @Autowired(required = false)
    private List<EventSource> eventSources;

    @Bean
    @ConditionalOnBean({EventSource.class, EventConsumer.class})
    public EventSourceConsumerProcess eventSourceConsumerProcess() {
        return new EventSourceConsumerProcess(
                eventSources != null ? eventSources : emptyList(),
                eventConsumers != null ? eventConsumers : emptyList()
        );
    }
}
