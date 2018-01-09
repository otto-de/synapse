package de.otto.edison.eventsourcing.annotation;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.eventsourcing.CompactingKinesisEventSource;
import de.otto.edison.eventsourcing.SnapshotEventSourceBuilder;
import de.otto.edison.eventsourcing.configuration.EventSourcingConfiguration;
import de.otto.edison.eventsourcing.consumer.Event;
import de.otto.edison.eventsourcing.consumer.MethodInvokingEventConsumer;
import de.otto.edison.eventsourcing.s3.SnapshotEventSource;
import org.junit.After;
import org.junit.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.security.crypto.encrypt.TextEncryptor;

import static org.assertj.core.api.Assertions.assertThat;

public class EventSourceConsumerBeanPostProcessorTest {

    private AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();

    @After
    public void close() {
        if (this.context != null) {
            this.context.close();
        }
    }

    static class TestConfiguration {
        @Bean
        public TestConsumer test() {
            return new TestConsumer();
        }
    }

    static class TestConfigurationDifferentPayload {
        @Bean
        public TestConsumerWithSameStreamNameAndDifferentPayload test() {
            return new TestConsumerWithSameStreamNameAndDifferentPayload();
        }
    }

    static class TestConfigurationCustomEventSource {
        @Bean
        public TestConsumerWithSnapshotEventSource test() {
            return new TestConsumerWithSnapshotEventSource();
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
    public void shouldRegisterMultipleConsumerForOneEventSourceAndDifferentPayloadType() {
        context.register(TestTextEncryptor.class);
        context.register(ObjectMapper.class);
        context.register(TestConfigurationDifferentPayload.class);
        context.register(EventSourcingConfiguration.class);
        context.refresh();

        assertThat(context.containsBean("firstConsumer")).isTrue();
        assertThat(context.getType("firstConsumer")).isEqualTo(MethodInvokingEventConsumer.class);
        assertThat(context.containsBean("secondConsumer")).isTrue();
        assertThat(context.getType("secondConsumer")).isEqualTo(MethodInvokingEventConsumer.class);

        assertThat(context.containsBean("someStreamEventSource")).isTrue();
        assertThat(context.getType("someStreamEventSource")).isEqualTo(CompactingKinesisEventSource.class);

        assertThat(context.getBeanNamesForType(CompactingKinesisEventSource.class).length).isEqualTo(1);
    }

    @Test
    public void shouldRegisterEventConsumerWithCustomEventSource() {
        context.register(TestTextEncryptor.class);
        context.register(ObjectMapper.class);
        context.register(TestConfigurationCustomEventSource.class);
        context.register(EventSourcingConfiguration.class);
        context.refresh();

        assertThat(context.containsBean("someStreamEventSource")).isTrue();
        assertThat(context.getType("someStreamEventSource")).isEqualTo(SnapshotEventSource.class);
    }

    static class TestTextEncryptor implements TextEncryptor {
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

    static class TestConsumerWithSameStreamNameAndDifferentPayload {
        @EventSourceConsumer(
                name = "firstConsumer",
                streamName = "some-stream",
                keyPattern = "apple.*",
                payloadType = String.class)
        public void first(Event<String> event) {
        }

        @EventSourceConsumer(
                name = "secondConsumer",
                streamName = "some-stream",
                keyPattern = "banana.*",
                payloadType = Integer.class)
        public void second(Event<String> event) {
        }

    }

    static class TestConsumerWithSnapshotEventSource {
        @EventSourceConsumer(
                name = "firstConsumer",
                streamName = "some-stream",
                payloadType = String.class,
                builder = SnapshotEventSourceBuilder.class)
        public void first(Event<String> event) {
        }
    }

}