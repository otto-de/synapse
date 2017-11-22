package de.otto.edison.eventsourcing.annotation;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.eventsourcing.CompactingKinesisEventSource;
import de.otto.edison.eventsourcing.configuration.EventSourcingConfiguration;
import de.otto.edison.eventsourcing.consumer.Event;
import de.otto.edison.eventsourcing.consumer.MethodInvokingEventConsumer;
import org.junit.After;
import org.junit.Test;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.crypto.encrypt.TextEncryptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

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
        public void first(Event<String> event) {
        }

        @EventSourceConsumer(
                name = "secondConsumer",
                streamName = "some-stream",
                payloadType = String.class)
        public void second(Event<String> event) {
        }

        @EventSourceConsumer(
                name = "thirdConsumer",
                streamName = "other-stream",
                payloadType = String.class)
        public void third(Event<String> event) {
        }
    }


    static class FailingTestConsumer {
        @EventSourceConsumer(
                name = "firstConsumer",
                streamName = "some-stream",
                payloadType = String.class)
        public void first(Event<String> event) {
        }

        @EventSourceConsumer(
                name = "secondConsumer",
                streamName = "some-stream",
                payloadType = Integer.class)
        public void second(Event<Integer> event) {
        }
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
        context.register(TestTextEncryptor.class);
        context.register(ObjectMapper.class);
        context.register(TestConfiguration.class);
        context.register(EventSourcingConfiguration.class);

        context.refresh();
        assertThat(context.containsBean("firstConsumer")).isTrue();
        assertThat(context.getType("firstConsumer")).isEqualTo(MethodInvokingEventConsumer.class);
        assertThat(context.containsBean("secondConsumer")).isTrue();
        assertThat(context.getType("secondConsumer")).isEqualTo(MethodInvokingEventConsumer.class);

        assertThat(context.containsBean("someStreamEventSource")).isTrue();
        assertThat(context.getType("someStreamEventSource")).isEqualTo(CompactingKinesisEventSource.class);

        assertThat(context.containsBean("otherStreamEventSource")).isTrue();
        assertThat(context.getType("otherStreamEventSource")).isEqualTo(CompactingKinesisEventSource.class);
    }

    @Test
    public void shouldFailRegisteringEventConsumersWithSameNameAndDifferentType() {
        try {
            context.register(TestTextEncryptor.class);
            context.register(ObjectMapper.class);
            context.register(FailingTestConsumer.class);
            context.register(EventSourcingConfiguration.class);
            context.refresh();
            fail();
        } catch (BeanCreationException e) {
            assertThat(e.getBeanName()).isEqualTo("eventSourceConsumerBeanPostProcessorTest.FailingTestConsumer");
            assertThat(e.getCause().toString()).isEqualTo("java.lang.IllegalStateException: Cannot register consumers for same streamName \"some-stream\" but with different payloadType");
        }
    }

    public static class TestTextEncryptor implements TextEncryptor {
        public TestTextEncryptor() {

        }
        @Override
        public String encrypt(String text) {
            return text;
        }

        @Override
        public String decrypt(String encryptedText) {
            return encryptedText;
        }
    }

}