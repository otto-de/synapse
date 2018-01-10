package de.otto.edison.eventsourcing.annotation;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.eventsourcing.DelegateEventSource;
import de.otto.edison.eventsourcing.configuration.EventSourcingConfiguration;
import de.otto.edison.eventsourcing.consumer.Event;
import de.otto.edison.eventsourcing.consumer.EventConsumer;
import de.otto.edison.eventsourcing.consumer.MethodInvokingEventConsumer;
import org.junit.After;
import org.junit.Test;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.security.crypto.encrypt.TextEncryptor;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class EventSourceConsumerBeanPostProcessorTest {

    private AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();

    @After
    public void close() {
        if (this.context != null) {
            this.context.close();
        }
    }

    @Test
    public void shouldRegisterMultipleEventConsumers() {
        context.register(TestTextEncryptor.class);
        context.register(ObjectMapper.class);
        context.register(ThreeConsumersAtTwoEventSourcesConfiguration.class);
        context.register(EventSourcingConfiguration.class);
        context.refresh();

        final DelegateEventSource someStreamEventSource = context.getBean("someStreamEventSource", DelegateEventSource.class);
        final List<EventConsumer<?>> eventConsumers = someStreamEventSource.registeredConsumers().getAll();
        assertThat(eventConsumers).hasSize(2);
        assertThat(eventConsumers.get(0)).isInstanceOf(MethodInvokingEventConsumer.class);
        assertThat(eventConsumers.get(1)).isInstanceOf(MethodInvokingEventConsumer.class);

        final DelegateEventSource otherStreamEventSource = context.getBean("otherStreamTestSourceWithCustomName", DelegateEventSource.class);
        final List<EventConsumer<?>> otherEventConsumers = otherStreamEventSource.registeredConsumers().getAll();
        assertThat(otherEventConsumers).hasSize(1);
        assertThat(otherEventConsumers.get(0)).isInstanceOf(MethodInvokingEventConsumer.class);
    }

    @Test(expected = BeanCreationException.class)
    public void shouldFailToRegisterConsumerBecauseOfMissingEventSource() {
        context.register(TestTextEncryptor.class);
        context.register(ObjectMapper.class);
        context.register(TestConfigurationWithMissingEventSource.class);
        context.register(EventSourcingConfiguration.class);
        context.refresh();
    }

    /**
     * If there is a Consumer for stream x and the consumer does refer to a single EventSource instance,
     * registration should succeed, if there are _multiple_ EventSources for stream x with matching name.
     */
    @Test
    public void shouldRegisterConsumerAtSpecifiedEventSource() {
        context.register(TestTextEncryptor.class);
        context.register(ObjectMapper.class);
        context.register(TwoEventSourcesWithSameStreamAndSecificConsumerConfiguration.class);
        context.register(EventSourcingConfiguration.class);
        context.refresh();

        final DelegateEventSource someStreamEventSource = context.getBean("someTestEventSource", DelegateEventSource.class);
        final List<EventConsumer<?>> eventConsumers = someStreamEventSource.registeredConsumers().getAll();
        assertThat(eventConsumers).hasSize(1);
        assertThat(eventConsumers.get(0)).isInstanceOf(MethodInvokingEventConsumer.class);
    }


    /**
     * If there is a Consumer for stream x and the consumer does not refer to a single EventSource instance,
     * registration should fail, if there are _multiple_ EventSources for stream x.
     */
    @Test(expected = BeanCreationException.class)
    public void shouldFailToRegisterConsumerWithoutReferenceToEventSourceIfThereAreMultipleOptions() {
        context.register(TestTextEncryptor.class);
        context.register(ObjectMapper.class);
        context.register(TwoEventSourcesWithSameStreamAndUnspecificConsumerConfiguration.class);
        context.register(EventSourcingConfiguration.class);
        context.refresh();
    }

    @Test
    public void shouldCreateEventConsumerWithSpecificPayloadType() {
        context.register(TestTextEncryptor.class);
        context.register(ObjectMapper.class);
        context.register(TestConfigurationDifferentPayload.class);
        context.register(EventSourcingConfiguration.class);
        context.refresh();

        final DelegateEventSource someStreamEventSource = context.getBean("someStreamEventSource", DelegateEventSource.class);
        final List<EventConsumer<?>> eventConsumers = someStreamEventSource.registeredConsumers().getAll();
        assertThat(eventConsumers).hasSize(2);
        assertThat(eventConsumers.get(0).payloadType()).isEqualTo(String.class);
        assertThat(eventConsumers.get(1).payloadType()).isEqualTo(Integer.class);
    }

    @Test
    public void shouldRegisterEventConsumerWithSpecificKeyPattern() {
        context.register(TestTextEncryptor.class);
        context.register(ObjectMapper.class);
        context.register(TestConfigurationDifferentPayload.class);
        context.register(EventSourcingConfiguration.class);
        context.refresh();

        final DelegateEventSource someStreamEventSource = context.getBean("someStreamEventSource", DelegateEventSource.class);
        final List<EventConsumer<?>> eventConsumers = someStreamEventSource.registeredConsumers().getAll();
        assertThat(eventConsumers).hasSize(2);
        assertThat(eventConsumers.get(0).keyPattern().pattern()).isEqualTo("apple.*");
        assertThat(eventConsumers.get(1).keyPattern().pattern()).isEqualTo("banana.*");
    }

    @EnableEventSource(streamName = "some-stream")
    @EnableEventSource(name = "otherStreamTestSourceWithCustomName", streamName = "other-stream")
    static class ThreeConsumersAtTwoEventSourcesConfiguration {
        @Bean
        public TestConsumer test() {
            return new TestConsumer();
        }
    }

    @EnableEventSource(name = "someTestEventSource", streamName = "some-stream")
    @EnableEventSource(name = "otherTestEventSource", streamName = "some-stream")
    static class TwoEventSourcesWithSameStreamAndUnspecificConsumerConfiguration {
        @Bean
        public SingleUnspecificConsumer test() {
            return new SingleUnspecificConsumer();
        }
    }

    @EnableEventSource(name = "someTestEventSource", streamName = "some-stream")
    @EnableEventSource(name = "otherTestEventSource", streamName = "some-stream")
    static class TwoEventSourcesWithSameStreamAndSecificConsumerConfiguration {
        @Bean
        public SingleSpecificConsumer test() {
            return new SingleSpecificConsumer();
        }
    }

    @EnableEventSource(streamName = "some-stream")
    static class TestConfigurationDifferentPayload {
        @Bean
        public TestConsumerWithSameStreamNameAndDifferentPayload test() {
            return new TestConsumerWithSameStreamNameAndDifferentPayload();
        }
    }

    static class TestConfigurationWithMissingEventSource{
        @Bean
        public TestConsumerWithSnapshotEventSource test() {
            return new TestConsumerWithSnapshotEventSource();
        }
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


    static class SingleUnspecificConsumer {
        @EventSourceConsumer(
                streamName = "some-stream",
                payloadType = String.class)
        public void first(Event<String> event) {
        }

    }

    static class SingleSpecificConsumer {
        @EventSourceConsumer(
                eventSource = "someTestEventSource",
                streamName = "some-stream",
                payloadType = String.class)
        public void first(Event<String> event) {
        }

    }

    static class TestConsumer {
        @EventSourceConsumer(
                streamName = "some-stream",
                payloadType = String.class)
        public void first(Event<String> event) {
        }

        @EventSourceConsumer(
                streamName = "some-stream",
                payloadType = String.class)
        public void second(Event<String> event) {
        }

        @EventSourceConsumer(
                streamName = "other-stream",
                payloadType = String.class)
        public void third(Event<String> event) {
        }
    }

    static class TestConsumerWithSameStreamNameAndDifferentPayload {
        @EventSourceConsumer(
                streamName = "some-stream",
                keyPattern = "apple.*",
                payloadType = String.class)
        public void first(Event<String> event) {
        }

        @EventSourceConsumer(
                streamName = "some-stream",
                keyPattern = "banana.*",
                payloadType = Integer.class)
        public void second(Event<String> event) {
        }

    }

    static class TestConsumerWithSnapshotEventSource {
        @EventSourceConsumer(
                streamName = "custom-test-stream",
                payloadType = String.class)
        public void first(Event<String> event) {
        }
    }

}