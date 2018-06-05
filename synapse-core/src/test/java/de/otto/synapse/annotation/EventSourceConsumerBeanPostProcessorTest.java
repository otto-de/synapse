package de.otto.synapse.annotation;

import de.otto.synapse.configuration.InMemoryTestConfiguration;
import de.otto.synapse.configuration.SynapseAutoConfiguration;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.consumer.MethodInvokingMessageConsumer;
import de.otto.synapse.eventsource.DelegateEventSource;
import de.otto.synapse.message.Message;
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
        context.register(SynapseAutoConfiguration.class);
        context.register(ThreeConsumersAtTwoEventSourcesConfiguration.class);
        context.register(InMemoryTestConfiguration.class);
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
        context.register(SynapseAutoConfiguration.class);
        context.register(TestConfigurationWithMissingEventSource.class);
        context.register(InMemoryTestConfiguration.class);
        context.refresh();
    }

    @Test
    public void shouldCreateEventConsumerWithSpecificPayloadType() {
        context.register(SynapseAutoConfiguration.class);
        context.register(TestConfigurationDifferentPayload.class);
        context.register(InMemoryTestConfiguration.class);
        context.refresh();

        final DelegateEventSource someStreamEventSource = context.getBean("testEventSource", DelegateEventSource.class);
        final List<MessageConsumer<?>> messageConsumers = someStreamEventSource.getMessageDispatcher().getAll();
        assertThat(messageConsumers).hasSize(2);
        Set<Object> payloadTYpes = messageConsumers.stream().map(MessageConsumer::payloadType).collect(toSet());
        assertThat(payloadTYpes).containsExactlyInAnyOrder(String.class, Integer.class);
    }


    @Test
    public void shouldRegisterEventConsumerWithSpecificKeyPattern() {
        context.register(SynapseAutoConfiguration.class);
        context.register(TestConfigurationDifferentPayload.class);
        context.register(InMemoryTestConfiguration.class);
        context.refresh();

        final DelegateEventSource someStreamEventSource = context.getBean("testEventSource", DelegateEventSource.class);
        final List<MessageConsumer<?>> messageConsumers = someStreamEventSource.getMessageDispatcher().getAll();
        assertThat(messageConsumers).hasSize(2);
        Set<String> pattern = messageConsumers.stream().map(consumer -> consumer.keyPattern().pattern()).collect(toSet());
        assertThat(pattern).containsExactlyInAnyOrder("apple.*", "banana.*");
    }

    @EnableEventSource(name = "testEventSource", channelName = "some-stream")
    @EnableEventSource(name = "otherStreamTestSource", channelName = "other-stream")
    static class ThreeConsumersAtTwoEventSourcesConfiguration {
        @Bean
        public TestConsumer test() {
            return new TestConsumer();
        }
    }

    @EnableEventSource(name = "someTestEventSource", channelName = "some-stream")
    @EnableEventSource(name = "otherTestEventSource", channelName = "some-stream")
    static class TwoEventSourcesWithSameStreamAndUnspecificConsumerConfiguration {
        @Bean
        public SingleUnspecificConsumer test() {
            return new SingleUnspecificConsumer();
        }
    }

    @EnableEventSource(name = "testEventSource", channelName = "some-stream")
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
