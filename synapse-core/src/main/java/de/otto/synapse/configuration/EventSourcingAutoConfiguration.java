package de.otto.synapse.configuration;

import de.otto.synapse.annotation.EventSourceConsumerBeanPostProcessor;
import de.otto.synapse.eventsource.DefaultEventSource;
import de.otto.synapse.eventsource.EventSource;
import de.otto.synapse.eventsource.EventSourceBuilder;
import de.otto.synapse.eventsource.EventSourceConsumerProcess;
import de.otto.synapse.messagestore.MessageStoreFactory;
import de.otto.synapse.messagestore.SnapshotMessageStore;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Role;

import java.util.List;

import static org.slf4j.LoggerFactory.getLogger;
import static org.springframework.beans.factory.config.BeanDefinition.ROLE_INFRASTRUCTURE;

@Import(SynapseAutoConfiguration.class)
@EnableConfigurationProperties(SynapseProperties.class)
public class EventSourcingAutoConfiguration {

    private static final Logger LOG = getLogger(EventSourcingAutoConfiguration.class);

    @Autowired(required = false)
    private List<EventSource> eventSources;

    @Bean
    @ConditionalOnMissingBean
    public EventSourceBuilder eventSourceBuilder(final MessageStoreFactory<SnapshotMessageStore> snapshotMessageStoreFactory) {
        return (messageLog) -> {
            final SnapshotMessageStore messageStore = snapshotMessageStoreFactory.createMessageStoreFor("snapshot", messageLog.getChannelName());
            return new DefaultEventSource(messageStore, messageLog);
        };
    }

    @Bean
    @ConditionalOnProperty(
            prefix = "synapse",
            name = "consumer-process.enabled",
            havingValue = "true",
            matchIfMissing = true)
    public EventSourceConsumerProcess eventSourceConsumerProcess() {
        return new EventSourceConsumerProcess(eventSources);
    }

    @Bean
    @Role(ROLE_INFRASTRUCTURE)
    public EventSourceConsumerBeanPostProcessor eventSourceConsumerAnnotationBeanPostProcessor() {
        return new EventSourceConsumerBeanPostProcessor();
    }

}
