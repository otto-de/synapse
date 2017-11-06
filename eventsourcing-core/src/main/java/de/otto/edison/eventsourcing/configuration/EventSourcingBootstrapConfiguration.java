package de.otto.edison.eventsourcing.configuration;

import de.otto.edison.eventsourcing.annotation.EventSourceConsumerBeanPostProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Role;

import static org.springframework.beans.factory.config.BeanDefinition.ROLE_INFRASTRUCTURE;

@Configuration
public class EventSourcingBootstrapConfiguration {

    @Bean
    @Role(ROLE_INFRASTRUCTURE)
    public EventSourceConsumerBeanPostProcessor eventSourceConsumerAnnotationBeanPostProcessor() {
        return new EventSourceConsumerBeanPostProcessor();
    }

}
