package de.otto.synapse.annotation;

import de.otto.synapse.configuration.InMemoryMessageLogTestConfiguration;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.consumer.MethodInvokingMessageConsumer;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpoint;
import de.otto.synapse.message.Message;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

import java.util.List;
import java.util.Set;

import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;

public class MessageLogConsumerBeanPostProcessorTest {

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
    public void shouldRegisterMultipleLogConsumers() {
        context.register(ThreeConsumersAtTwoLogsConfiguration.class);
        context.register(InMemoryMessageLogTestConfiguration.class);
        context.refresh();

        final MessageLogReceiverEndpoint someLog = context.getBean("testLog", MessageLogReceiverEndpoint.class);
        final List<MessageConsumer<?>> messageConsumers = someLog.getMessageDispatcher().getAll();
        assertThat(messageConsumers).hasSize(2);
        assertThat(messageConsumers.get(0)).isInstanceOf(MethodInvokingMessageConsumer.class);
        assertThat(messageConsumers.get(1)).isInstanceOf(MethodInvokingMessageConsumer.class);

        final MessageLogReceiverEndpoint otherLog = context.getBean("otherLog", MessageLogReceiverEndpoint.class);
        final List<MessageConsumer<?>> otherMessageConsumers = otherLog.getMessageDispatcher().getAll();
        assertThat(otherMessageConsumers).hasSize(1);
        assertThat(otherMessageConsumers.get(0)).isInstanceOf(MethodInvokingMessageConsumer.class);
    }

    @Test(expected = BeanCreationException.class)
    public void shouldFailToRegisterConsumerBecauseOfMissingMessageQueue() {
        context.register(TestConfigurationWithMissingQueue.class);
        context.register(InMemoryMessageLogTestConfiguration.class);
        context.refresh();
    }

    @Test
    public void shouldCreateLogConsumerWithSpecificPayloadType() {
        context.register(TestConfigurationDifferentPayload.class);
        context.register(InMemoryMessageLogTestConfiguration.class);
        context.refresh();

        final MessageLogReceiverEndpoint testLog = context.getBean("testLog", MessageLogReceiverEndpoint.class);
        final List<MessageConsumer<?>> messageConsumers = testLog.getMessageDispatcher().getAll();
        assertThat(messageConsumers).hasSize(2);
        Set<Object> payloadTYpes = messageConsumers.stream().map(MessageConsumer::payloadType).collect(toSet());
        assertThat(payloadTYpes).containsExactlyInAnyOrder(String.class, Integer.class);
    }


    @Test
    public void shouldRegisterQueueConsumerWithSpecificKeyPattern() {
        context.register(TestConfigurationDifferentPayload.class);
        context.register(InMemoryMessageLogTestConfiguration.class);
        context.refresh();

        final MessageLogReceiverEndpoint testLog = context.getBean("testLog", MessageLogReceiverEndpoint.class);
        final List<MessageConsumer<?>> messageConsumers = testLog.getMessageDispatcher().getAll();
        assertThat(messageConsumers).hasSize(2);
        Set<String> pattern = messageConsumers.stream().map(consumer -> consumer.keyPattern().pattern()).collect(toSet());
        assertThat(pattern).containsExactlyInAnyOrder("apple.*", "banana.*");
    }

    @EnableMessageLogReceiverEndpoint(name = "testLog", channelName = "some-channel")
    @EnableMessageLogReceiverEndpoint(name = "otherLog", channelName = "other-channel")
    static class ThreeConsumersAtTwoLogsConfiguration {
        @Bean
        public TestConsumer test() {
            return new TestConsumer();
        }
    }

    @EnableMessageLogReceiverEndpoint(name = "testLog", channelName = "some-channel")
    static class TestConfigurationDifferentPayload {
        @Bean
        public TestConsumerWithSameChannelNameAndDifferentPayload test() {
            return new TestConsumerWithSameChannelNameAndDifferentPayload();
        }
    }

    @Import({
            InMemoryMessageLogTestConfiguration.class,
            MessageLogReceiverEndpointBeanRegistrar.class})
    static class TestConfigurationWithMissingQueue {
        @Bean
        public SingleTestConsumer test() {
            return new SingleTestConsumer();
        }
    }

    static class TestConsumer {
        @MessageLogConsumer(
                endpointName= "testLog",
                payloadType = String.class)
        public void first(Message<String> message) {
        }

        @MessageLogConsumer(
                endpointName = "testLog",
                payloadType = String.class)
        public void second(Message<String> message) {
        }

        @MessageLogConsumer(
                endpointName = "otherLog",
                payloadType = String.class)
        public void third(Message<String> message) {
        }
    }

    static class TestConsumerWithSameChannelNameAndDifferentPayload {
        @MessageLogConsumer(
                endpointName = "testLog",
                keyPattern = "apple.*",
                payloadType = String.class)
        public void first(Message<String> message) {
        }

        @MessageLogConsumer(
                endpointName = "testLog",
                keyPattern = "banana.*",
                payloadType = Integer.class)
        public void second(Message<String> message) {
        }

    }

    static class SingleTestConsumer{
        @MessageLogConsumer(
                endpointName = "someLog",
                payloadType = String.class)
        public void first(Message<String> message) {
        }
    }

}
