package de.otto.synapse.annotation;

import de.otto.synapse.configuration.EventSourcingAutoConfiguration;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.consumer.MethodInvokingMessageConsumer;
import de.otto.synapse.eventsource.DelegateEventSource;
import de.otto.synapse.message.Message;
import de.otto.synapse.testsupport.InMemoryEventSourceConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;

import java.util.List;
import java.util.Set;

import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;

public class EventSourceConsumerBeanPostProcessorTest {

    private AnnotationConfigApplicationContext context;

    @Before
    public void init() {
        context = new AnnotationConfigApplicationContext();
    }

    @After
    public void close() {
        if (this.context != null) {
            this.context.close();
        }
    }

    @Test
    public void shouldRegisterMultipleEventConsumers() {
        context.register(EventSourcingAutoConfiguration.class);
        context.register(ThreeConsumersAtTwoEventSourcesConfiguration.class);
        context.register(InMemoryEventSourceConfiguration.class);
        context.refresh();

        final DelegateEventSource someStreamEventSource = context.getBean("testEventSource", DelegateEventSource.class);
        final List<MessageConsumer<?>> messageConsumers = someStreamEventSource.getMessageDispatcher().getAll();
        assertThat(messageConsumers).hasSize(2);
        assertThat(messageConsumers.get(0)).isInstanceOf(MethodInvokingMessageConsumer.class);
        assertThat(messageConsumers.get(1)).isInstanceOf(MethodInvokingMessageConsumer.class);

        final DelegateEventSource otherStreamEventSource = context.getBean("otherStreamTestSource", DelegateEventSource.class);
        final List<MessageConsumer<?>> otherMessageConsumers = otherStreamEventSource.getMessageDispatcher().getAll();
        assertThat(otherMessageConsumers).hasSize(1);
        assertThat(otherMessageConsumers.get(0)).isInstanceOf(MethodInvokingMessageConsumer.class);
    }

    @Test(expected = BeanCreationException.class)
    public void shouldFailToRegisterConsumerBecauseOfMissingEventSource() {
        context.register(EventSourcingAutoConfiguration.class);
        context.register(TestConfigurationWithMissingEventSource.class);
        context.register(InMemoryEventSourceConfiguration.class);
        context.refresh();
    }

    /**
     * If there is a Consumer for stream x and the consumer does refer to a single EventSource instance,
     * registration should succeed, if there are _multiple_ EventSources for stream x with matching name.
     */
    @Test
    public void shouldRegisterConsumerAtSpecifiedEventSource() {
        context.register(EventSourcingAutoConfiguration.class);
        context.register(TwoEventSourcesWithSameStreamAndSecificConsumerConfiguration.class);
        context.register(InMemoryEventSourceConfiguration.class);
        context.refresh();

        final DelegateEventSource someStreamEventSource = context.getBean("someTestEventSource", DelegateEventSource.class);
        final List<MessageConsumer<?>> messageConsumers = someStreamEventSource.getMessageDispatcher().getAll();
        assertThat(messageConsumers).hasSize(1);
        assertThat(messageConsumers.get(0)).isInstanceOf(MethodInvokingMessageConsumer.class);
    }

    @Test
    public void shouldCreateEventConsumerWithSpecificPayloadType() {
        context.register(EventSourcingAutoConfiguration.class);
        context.register(TestConfigurationDifferentPayload.class);
        context.register(InMemoryEventSourceConfiguration.class);
        context.refresh();

        final DelegateEventSource someStreamEventSource = context.getBean("testEventSource", DelegateEventSource.class);
        final List<MessageConsumer<?>> messageConsumers = someStreamEventSource.getMessageDispatcher().getAll();
        assertThat(messageConsumers).hasSize(2);
        Set<Object> payloadTYpes = messageConsumers.stream().map(MessageConsumer::payloadType).collect(toSet());
        assertThat(payloadTYpes).containsExactlyInAnyOrder(String.class, Integer.class);
    }


    @Test
    public void shouldRegisterEventConsumerWithSpecificKeyPattern() {
        context.register(EventSourcingAutoConfiguration.class);
        context.register(TestConfigurationDifferentPayload.class);
        context.register(InMemoryEventSourceConfiguration.class);
        context.refresh();

        final DelegateEventSource someStreamEventSource = context.getBean("testEventSource", DelegateEventSource.class);
        final List<MessageConsumer<?>> messageConsumers = someStreamEventSource.getMessageDispatcher().getAll();
        assertThat(messageConsumers).hasSize(2);
        Set<String> pattern = messageConsumers.stream().map(consumer -> consumer.keyPattern().pattern()).collect(toSet());
        assertThat(pattern).containsExactlyInAnyOrder("apple.*", "banana.*");
    }

    @EnableEventSource(name = "testEventSource", streamName = "some-stream", builder = "inMemEventSourceBuilder")
    @EnableEventSource(name = "otherStreamTestSource", streamName = "other-stream", builder = "inMemEventSourceBuilder")
    static class ThreeConsumersAtTwoEventSourcesConfiguration {
        @Bean
        public TestConsumer test() {
            return new TestConsumer();
        }
    }

    @EnableEventSource(name = "someTestEventSource", streamName = "some-stream", builder = "inMemEventSourceBuilder")
    @EnableEventSource(name = "otherTestEventSource", streamName = "some-stream", builder = "inMemEventSourceBuilder")
    static class TwoEventSourcesWithSameStreamAndUnspecificConsumerConfiguration {
        @Bean
        public SingleUnspecificConsumer test() {
            return new SingleUnspecificConsumer();
        }
    }

    @EnableEventSource(name = "someTestEventSource", streamName = "some-stream", builder = "inMemEventSourceBuilder")
    @EnableEventSource(name = "otherTestEventSource", streamName = "some-stream", builder = "inMemEventSourceBuilder")
    static class TwoEventSourcesWithSameStreamAndSecificConsumerConfiguration {
        @Bean
        public SingleSpecificConsumer test() {
            return new SingleSpecificConsumer();
        }
    }

    @EnableEventSource(name = "testEventSource", streamName = "some-stream", builder = "inMemEventSourceBuilder")
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

    static class SingleUnspecificConsumer {
        @EventSourceConsumer(
                eventSource = "someTestEventSource",
                payloadType = String.class)
        public void first(Message<String> message) {
        }

    }

    static class SingleSpecificConsumer {
        @EventSourceConsumer(
                eventSource = "someTestEventSource",
                payloadType = String.class)
        public void first(Message<String> message) {
        }

    }

    static class TestConsumer {
        @EventSourceConsumer(
                eventSource= "testEventSource",
                payloadType = String.class)
        public void first(Message<String> message) {
        }

        @EventSourceConsumer(
                eventSource = "testEventSource",
                payloadType = String.class)
        public void second(Message<String> message) {
        }

        @EventSourceConsumer(
                eventSource = "otherStreamTestSource",
                payloadType = String.class)
        public void third(Message<String> message) {
        }
    }

    static class TestConsumerWithSameStreamNameAndDifferentPayload {
        @EventSourceConsumer(
                eventSource = "testEventSource",
                keyPattern = "apple.*",
                payloadType = String.class)
        public void first(Message<String> message) {
        }

        @EventSourceConsumer(
                eventSource = "testEventSource",
                keyPattern = "banana.*",
                payloadType = Integer.class)
        public void second(Message<String> message) {
        }

    }

    static class TestConsumerWithSnapshotEventSource {
        @EventSourceConsumer(
                eventSource = "someTestEventSource",
                payloadType = String.class)
        public void first(Message<String> message) {
        }
    }

}
