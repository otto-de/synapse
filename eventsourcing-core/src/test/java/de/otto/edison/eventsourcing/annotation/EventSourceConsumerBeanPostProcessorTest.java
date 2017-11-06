package de.otto.edison.eventsourcing.annotation;

import de.otto.edison.eventsourcing.configuration.EventSourcingConfiguration;
import de.otto.edison.eventsourcing.consumer.Event;
import de.otto.edison.eventsourcing.consumer.MethodInvokingEventConsumer;
import org.junit.After;
import org.junit.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;

public class EventSourceConsumerBeanPostProcessorTest {

    private AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();

    @After
    public void close() {
        if (this.context != null) {
            this.context.close();
        }
    }

    static class TestConsumer {
        @EventSourceConsumer(
                name = "firstConsumer",
                streamName = "some-stream",
                payloadType = String.class)
        public void first(Event<String> event) {}

        @EventSourceConsumer(
                name = "secondConsumer",
                streamName = "some-stream",
                payloadType = String.class)
        public void second(Event<String> event) {}
    }

    @Configuration
    static class TestConfiguration {
        @Bean
        public TestConsumer test() {
            return new TestConsumer();
        }
    }

    @Test
    public void shouldRegisterEventConsumers() {
        context.register(TestConfiguration.class);
        context.register(EventSourcingConfiguration.class);
        context.refresh();
        assertThat(context.containsBean("firstConsumer")).isTrue();
        assertThat(context.getType("firstConsumer")).isEqualTo(MethodInvokingEventConsumer.class);
        assertThat(context.containsBean("secondConsumer")).isTrue();
        assertThat(context.getType("secondConsumer")).isEqualTo(MethodInvokingEventConsumer.class);
    }
}