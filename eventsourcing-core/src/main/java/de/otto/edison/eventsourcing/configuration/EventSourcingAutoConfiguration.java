package de.otto.edison.eventsourcing.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import de.otto.edison.eventsourcing.EventSourceBuilder;
import de.otto.edison.eventsourcing.annotation.EventSourceConsumerBeanPostProcessor;
import de.otto.edison.eventsourcing.consumer.EventSource;
import de.otto.edison.eventsourcing.consumer.EventSourceConsumerProcess;
import de.otto.edison.eventsourcing.inmemory.InMemoryEventSource;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Role;

import java.util.List;

import static org.slf4j.LoggerFactory.getLogger;
import static org.springframework.beans.factory.config.BeanDefinition.ROLE_INFRASTRUCTURE;

@Configuration
@EnableConfigurationProperties(ConsumerProcessProperties.class)
public class EventSourcingAutoConfiguration {

    @Autowired(required = false)
    private List<EventSource> eventSources;

    @Bean
    @ConditionalOnProperty(
            prefix = "edison.eventsourcing",
            name = "consumer-process.enabled",
            havingValue = "true",
            matchIfMissing = true)
    @ConditionalOnBean(EventSource.class)
    public EventSourceConsumerProcess eventSourceConsumerProcess() {
        return new EventSourceConsumerProcess(eventSources);
    }

    @Bean
    @ConditionalOnMissingBean(ObjectMapper.class)
    public ObjectMapper objectMapper() {
        return new ObjectMapper().registerModule(new JavaTimeModule());
    }

    @Bean
    @Role(ROLE_INFRASTRUCTURE)
    public EventSourceConsumerBeanPostProcessor eventSourceConsumerAnnotationBeanPostProcessor() {
        return new EventSourceConsumerBeanPostProcessor();
    }

}
