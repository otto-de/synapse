package de.otto.synapse.annotation;

import de.otto.synapse.configuration.InMemoryMessageQueueTestConfiguration;
import de.otto.synapse.endpoint.receiver.DelegateMessageQueueReceiverEndpoint;
import de.otto.synapse.endpoint.receiver.MessageQueueReceiverEndpoint;
import org.junit.After;
import org.junit.Test;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;

public class MessageQueueReceiverEndpointBeanRegistrarTest {

    private AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();

    @After
    public void close() {
        if (this.context != null) {
            this.context.close();
        }
    }

    @EnableMessageQueueReceiverEndpoint(name = "testQueue", channelName = "test-channel")
    private static class SingleQueueReceiverConfig {
    }

    @EnableMessageQueueReceiverEndpoint(channelName = "test-channel")
    private static class SingleUnnamedQueueReceiverConfig {
    }

    @EnableMessageQueueReceiverEndpoint(name = "broken", channelName = "some-channel")
    @EnableMessageQueueReceiverEndpoint(name = "broken", channelName = "some-channel")
    private static class RepeatableQueueReceiverConfigWithSameNames {
    }

    @EnableMessageQueueReceiverEndpoint(channelName = "some-channel")
    @EnableMessageQueueReceiverEndpoint(channelName = "other-channel")
    private static class RepeatableUnnamedQueueReceiverConfigWithDifferentChannels {
    }

    @EnableMessageQueueReceiverEndpoint(name = "firstQueue", channelName = "first-channel")
    @EnableMessageQueueReceiverEndpoint(name = "secondQueue", channelName = "${test.channel-name}")
    private static class RepeatableQueueReceiverConfig {
    }

    @Test
    public void shouldRegisterMessageQueueReceiverEndpointBean() {
        context.register(SingleQueueReceiverConfig.class);
        context.register(InMemoryMessageQueueTestConfiguration.class);
        context.refresh();

        assertThat(context.containsBean("testQueue")).isTrue();
    }

    @Test
    public void shouldRegisterMessageQueueReceiverEndpointWithNameDerivedFromChannelName() {
        context.register(SingleUnnamedQueueReceiverConfig.class);
        context.register(InMemoryMessageQueueTestConfiguration.class);
        context.refresh();

        assertThat(context.containsBean("testChannelMessageQueueReceiverEndpoint")).isTrue();
        final MessageQueueReceiverEndpoint receiverEndpoint = context.getBean("testChannelMessageQueueReceiverEndpoint", MessageQueueReceiverEndpoint.class);
        assertThat(receiverEndpoint.getChannelName()).isEqualTo("test-channel");
    }

    @Test(expected = BeanCreationException.class)
    public void shouldFailToRegisterMultipleQueuesForSameChannelNameWithSameName() {
        context.register(RepeatableQueueReceiverConfigWithSameNames.class);
        context.register(InMemoryMessageQueueTestConfiguration.class);
        context.refresh();
    }

    @Test
    public void shouldRegisterMultipleUnnamedQueuesForDifferentChannels() {
        context.register(RepeatableUnnamedQueueReceiverConfigWithDifferentChannels.class);
        context.register(InMemoryMessageQueueTestConfiguration.class);
        context.refresh();

        assertThat(context.getBean("someChannelMessageQueueReceiverEndpoint", MessageQueueReceiverEndpoint.class).getChannelName()).isEqualTo("some-channel");
        assertThat(context.getBean("otherChannelMessageQueueReceiverEndpoint", MessageQueueReceiverEndpoint.class).getChannelName()).isEqualTo("other-channel");
    }

    @Test
    public void shouldRegisterMultipleQueues() {
        context.register(RepeatableQueueReceiverConfig.class);
        context.register(InMemoryMessageQueueTestConfiguration.class);
        TestPropertyValues.of(
                "test.channel-name=second-channel"
        ).applyTo(context);
        context.refresh();

        assertThat(context.containsBean("firstQueue")).isTrue();
        assertThat(context.containsBean("secondQueue")).isTrue();

        final MessageQueueReceiverEndpoint first = context.getBean("firstQueue", MessageQueueReceiverEndpoint.class);
        assertThat(first.getChannelName()).isEqualTo("first-channel");
        assertThat(first).isInstanceOf(DelegateMessageQueueReceiverEndpoint.class);

        final MessageQueueReceiverEndpoint second = context.getBean("secondQueue", MessageQueueReceiverEndpoint.class);
        assertThat(second.getChannelName()).isEqualTo("second-channel");
        assertThat(second).isInstanceOf(DelegateMessageQueueReceiverEndpoint.class);
    }

}
