package de.otto.synapse.annotation.messagequeue;

import de.otto.synapse.configuration.InMemoryMessageQueueTestConfiguration;
import de.otto.synapse.configuration.MessageQueueReceiverEndpointAutoConfiguration;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.consumer.MethodInvokingMessageConsumer;
import de.otto.synapse.endpoint.receiver.MessageQueueReceiverEndpoint;
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

public class MessageQueueConsumerBeanPostProcessorTest {

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
    public void shouldRegisterMultipleQueueConsumers() {
        context.register(ThreeConsumersAtTwoQueuesConfiguration.class);
        context.register(InMemoryMessageQueueTestConfiguration.class);
        context.refresh();

        final MessageQueueReceiverEndpoint someQueue = context.getBean("testQueue", MessageQueueReceiverEndpoint.class);
        final List<MessageConsumer<?>> messageConsumers = someQueue.getMessageDispatcher().getAll();
        assertThat(messageConsumers).hasSize(2);
        assertThat(messageConsumers.get(0)).isInstanceOf(MethodInvokingMessageConsumer.class);
        assertThat(messageConsumers.get(1)).isInstanceOf(MethodInvokingMessageConsumer.class);

        final MessageQueueReceiverEndpoint otherQueue = context.getBean("otherQueue", MessageQueueReceiverEndpoint.class);
        final List<MessageConsumer<?>> otherMessageConsumers = otherQueue.getMessageDispatcher().getAll();
        assertThat(otherMessageConsumers).hasSize(1);
        assertThat(otherMessageConsumers.get(0)).isInstanceOf(MethodInvokingMessageConsumer.class);
    }

    @Test(expected = BeanCreationException.class)
    public void shouldFailToRegisterConsumerBecauseOfMissingMessageQueue() {
        context.register(TestConfigurationWithMissingQueue.class);
        context.register(InMemoryMessageQueueTestConfiguration.class);
        context.refresh();
    }

    @Test
    public void shouldCreateQueueConsumerWithSpecificPayloadType() {
        context.register(TestConfigurationDifferentPayload.class);
        context.register(InMemoryMessageQueueTestConfiguration.class);
        context.refresh();

        final MessageQueueReceiverEndpoint testQueue = context.getBean("testQueue", MessageQueueReceiverEndpoint.class);
        final List<MessageConsumer<?>> messageConsumers = testQueue.getMessageDispatcher().getAll();
        assertThat(messageConsumers).hasSize(2);
        Set<Object> payloadTYpes = messageConsumers.stream().map(MessageConsumer::payloadType).collect(toSet());
        assertThat(payloadTYpes).containsExactlyInAnyOrder(String.class, Integer.class);
    }


    @Test
    public void shouldRegisterQueueConsumerWithSpecificKeyPattern() {
        context.register(TestConfigurationDifferentPayload.class);
        context.register(InMemoryMessageQueueTestConfiguration.class);
        context.refresh();

        final MessageQueueReceiverEndpoint testQueue = context.getBean("testQueue", MessageQueueReceiverEndpoint.class);
        final List<MessageConsumer<?>> messageConsumers = testQueue.getMessageDispatcher().getAll();
        assertThat(messageConsumers).hasSize(2);
        Set<String> pattern = messageConsumers.stream().map(consumer -> consumer.keyPattern().pattern()).collect(toSet());
        assertThat(pattern).containsExactlyInAnyOrder("apple.*", "banana.*");
    }

    @EnableMessageQueueReceiverEndpoint(name = "testQueue", channelName = "some-channel")
    @EnableMessageQueueReceiverEndpoint(name = "otherQueue", channelName = "other-channel")
    static class ThreeConsumersAtTwoQueuesConfiguration {
        @Bean
        public TestConsumer test() {
            return new TestConsumer();
        }
    }

    @EnableMessageQueueReceiverEndpoint(name = "testQueue", channelName = "some-channel")
    static class TestConfigurationDifferentPayload {
        @Bean
        public TestConsumerWithSameChannelNameAndDifferentPayload test() {
            return new TestConsumerWithSameChannelNameAndDifferentPayload();
        }
    }

    @Import({
            MessageQueueReceiverEndpointAutoConfiguration.class,
            MessageQueueReceiverEndpointBeanRegistrar.class})
    static class TestConfigurationWithMissingQueue {
        @Bean
        public SingleTestConsumer test() {
            return new SingleTestConsumer();
        }
    }

    static class TestConsumer {
        @MessageQueueConsumer(
                endpointName= "testQueue",
                payloadType = String.class)
        public void first(Message<String> message) {
        }

        @MessageQueueConsumer(
                endpointName = "testQueue",
                payloadType = String.class)
        public void second(Message<String> message) {
        }

        @MessageQueueConsumer(
                endpointName = "otherQueue",
                payloadType = String.class)
        public void third(Message<String> message) {
        }
    }

    static class TestConsumerWithSameChannelNameAndDifferentPayload {
        @MessageQueueConsumer(
                endpointName = "testQueue",
                keyPattern = "apple.*",
                payloadType = String.class)
        public void first(Message<String> message) {
        }

        @MessageQueueConsumer(
                endpointName = "testQueue",
                keyPattern = "banana.*",
                payloadType = Integer.class)
        public void second(Message<String> message) {
        }

    }

    static class SingleTestConsumer{
        @MessageQueueConsumer(
                endpointName = "someQueue",
                payloadType = String.class)
        public void first(Message<String> message) {
        }
    }

}
