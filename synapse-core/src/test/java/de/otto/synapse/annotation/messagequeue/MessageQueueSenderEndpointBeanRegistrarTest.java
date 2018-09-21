package de.otto.synapse.annotation.messagequeue;

import de.otto.synapse.configuration.InMemoryMessageQueueTestConfiguration;
import de.otto.synapse.endpoint.sender.DelegateMessageQueueSenderEndpoint;
import de.otto.synapse.endpoint.sender.MessageSenderEndpoint;
import org.junit.After;
import org.junit.Test;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.boot.test.util.EnvironmentTestUtils.addEnvironment;

public class MessageQueueSenderEndpointBeanRegistrarTest {

    private AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();

    @After
    public void close() {
        if (this.context != null) {
            this.context.close();
        }
    }

    @EnableMessageQueueSenderEndpoint(name = "testQueue", channelName = "test-channel")
    private static class SingleQueueSenderConfig {
    }

    @EnableMessageQueueSenderEndpoint(channelName = "test-channel")
    private static class SingleUnnamedQueueSenderConfig {
    }

    @EnableMessageQueueSenderEndpoint(name = "broken", channelName = "some-channel")
    @EnableMessageQueueSenderEndpoint(name = "broken", channelName = "some-channel")
    private static class RepeatableQueueSenderConfigWithSameNames {
    }

    @EnableMessageQueueSenderEndpoint(channelName = "some-channel")
    @EnableMessageQueueSenderEndpoint(channelName = "other-channel")
    private static class RepeatableUnnamedQueueSenderConfigWithDifferentChannels {
    }

    @EnableMessageQueueSenderEndpoint(name = "firstQueue", channelName = "first-channel")
    @EnableMessageQueueSenderEndpoint(name = "secondQueue", channelName = "${test.channel-name}")
    private static class RepeatableQueueSenderConfig {
    }

    @Test
    public void shouldRegisterMessageQueueSenderEndpointBean() {
        context.register(SingleQueueSenderConfig.class);
        context.register(InMemoryMessageQueueTestConfiguration.class);
        context.refresh();

        assertThat(context.containsBean("testQueue")).isTrue();
        assertThat(context.getBean("testQueue", MessageSenderEndpoint.class)).isInstanceOf(DelegateMessageQueueSenderEndpoint.class);
    }

    @Test
    public void shouldRegisterMessageQueueSenderEndpointWithNameDerivedFromChannelName() {
        context.register(SingleUnnamedQueueSenderConfig.class);
        context.register(InMemoryMessageQueueTestConfiguration.class);
        context.refresh();

        assertThat(context.containsBean("testChannelMessageQueueSenderEndpoint")).isTrue();
        final MessageSenderEndpoint senderEndpoint = context.getBean("testChannelMessageQueueSenderEndpoint", MessageSenderEndpoint.class);
        assertThat(senderEndpoint.getChannelName()).isEqualTo("test-channel");
    }

    @Test(expected = BeanCreationException.class)
    public void shouldFailToRegisterMultipleQueueSendersForSameChannelNameWithSameName() {
        context.register(RepeatableQueueSenderConfigWithSameNames.class);
        context.register(InMemoryMessageQueueTestConfiguration.class);
        context.refresh();
    }

    @Test
    public void shouldRegisterMultipleUnnamedQueuesSendersForDifferentChannels() {
        context.register(RepeatableUnnamedQueueSenderConfigWithDifferentChannels.class);
        context.register(InMemoryMessageQueueTestConfiguration.class);
        context.refresh();

        assertThat(context.getBean("someChannelMessageQueueSenderEndpoint", MessageSenderEndpoint.class).getChannelName()).isEqualTo("some-channel");
        assertThat(context.getBean("otherChannelMessageQueueSenderEndpoint", MessageSenderEndpoint.class).getChannelName()).isEqualTo("other-channel");
    }

    @Test
    public void shouldRegisterMultipleQueueSenders() {
        context.register(RepeatableQueueSenderConfig.class);
        context.register(InMemoryMessageQueueTestConfiguration.class);
        addEnvironment(this.context,
                "test.channel-name=second-channel"
        );
        context.refresh();

        assertThat(context.containsBean("firstQueue")).isTrue();
        assertThat(context.containsBean("secondQueue")).isTrue();

        final MessageSenderEndpoint first = context.getBean("firstQueue", MessageSenderEndpoint.class);
        assertThat(first.getChannelName()).isEqualTo("first-channel");
        assertThat(first).isInstanceOf(DelegateMessageQueueSenderEndpoint.class);

        final MessageSenderEndpoint second = context.getBean("secondQueue", MessageSenderEndpoint.class);
        assertThat(second.getChannelName()).isEqualTo("second-channel");
        assertThat(second).isInstanceOf(DelegateMessageQueueSenderEndpoint.class);
    }

}
