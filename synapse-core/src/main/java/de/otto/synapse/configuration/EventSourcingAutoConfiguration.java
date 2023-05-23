package de.otto.synapse.configuration;

import de.otto.synapse.annotation.EventSourceConsumerBeanPostProcessor;
import de.otto.synapse.channel.selector.MessageLog;
import de.otto.synapse.eventsource.DefaultEventSourceBuilder;
import de.otto.synapse.eventsource.EventSource;
import de.otto.synapse.eventsource.EventSourceBuilder;
import de.otto.synapse.eventsource.EventSourceConsumerProcess;
import de.otto.synapse.messagestore.MessageStoreFactory;
import de.otto.synapse.messagestore.SnapshotMessageStore;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.*;

import java.util.List;

import static de.otto.synapse.messagestore.MessageStores.emptyMessageStore;
import static org.slf4j.LoggerFactory.getLogger;
import static org.springframework.beans.factory.config.BeanDefinition.ROLE_INFRASTRUCTURE;

@Configuration
@Import(SynapseAutoConfiguration.class)
public class EventSourcingAutoConfiguration {

    private static final Logger LOG = getLogger(EventSourcingAutoConfiguration.class);

    @Bean
    @ConditionalOnMissingBean(name = "defaultEventSourceBuilder")
    @ConditionalOnBean(name = "snapshotMessageStoreFactory")
    public EventSourceBuilder defaultEventSourceBuilder(final MessageStoreFactory<SnapshotMessageStore> snapshotMessageStoreFactory) {
        return new DefaultEventSourceBuilder(snapshotMessageStoreFactory, MessageLog.class);
    }

    @Bean
    @ConditionalOnMissingBean(name = {"defaultEventSourceBuilder", "snapshotMessageStoreFactory"})
    public EventSourceBuilder fallbackEventSourceBuilder() {
        LOG.info("No MessageStoreFactory is configured. Falling back to EventStoreBuilder w/o Snapshot MessageStore");
        return new DefaultEventSourceBuilder((_x) -> emptyMessageStore(), MessageLog.class);
    }

    @Bean
    @ConditionalOnProperty(
            prefix = "synapse",
            name = "consumer-process.enabled",
            havingValue = "true",
            matchIfMissing = true)
    public EventSourceConsumerProcess eventSourceConsumerProcess(@Autowired(required = false) List<EventSource> eventSources) {
        return new EventSourceConsumerProcess(eventSources);
    }

    @Bean
    @Role(ROLE_INFRASTRUCTURE)
    public EventSourceConsumerBeanPostProcessor eventSourceConsumerAnnotationBeanPostProcessor() {
        return new EventSourceConsumerBeanPostProcessor();
    }

}
