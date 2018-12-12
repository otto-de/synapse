package de.otto.synapse.annotation;

import de.otto.synapse.channel.selector.MessageLog;
import de.otto.synapse.configuration.InMemoryMessageQueueTestConfiguration;
import de.otto.synapse.endpoint.sender.DelegateMessageSenderEndpoint;
import de.otto.synapse.endpoint.sender.MessageSenderEndpoint;
import org.junit.After;
import org.junit.Test;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.boot.test.util.EnvironmentTestUtils.addEnvironment;

public class MessageSenderEndpointBeanRegistrarTest {

    private AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();

    @After
    public void close() {
        if (this.context != null) {
            this.context.close();
        }
    }

    @EnableMessageSenderEndpoint(name = "testQueue", channelName = "test-channel", selector = MessageLog.class)
    private static class SingleQueueSenderConfig {
    }

    @EnableMessageSenderEndpoint(channelName = "test-channel", selector = MessageLog.class)
    private static class SingleUnnamedQueueSenderConfig {
    }

    @EnableMessageSenderEndpoint(name = "broken", channelName = "some-channel", selector = MessageLog.class)
    @EnableMessageSenderEndpoint(name = "broken", channelName = "some-channel", selector = MessageLog.class)
    private static class RepeatableQueueSenderConfigWithSameNames {
    }

    @EnableMessageSenderEndpoint(channelName = "some-channel", selector = MessageLog.class)
    @EnableMessageSenderEndpoint(channelName = "other-channel", selector = MessageLog.class)
    private static class RepeatableUnnamedQueueSenderConfigWithDifferentChannels {
    }

    @EnableMessageSenderEndpoint(name = "firstQueue", channelName = "first-channel", selector = MessageLog.class)
    @EnableMessageSenderEndpoint(name = "secondQueue", channelName = "${test.channel-name}", selector = MessageLog.class)
    private static class RepeatableQueueSenderConfig {
    }

    @Test
    public void shouldRegisterMessageQueueSenderEndpointBean() {
        context.register(SingleQueueSenderConfig.class);
        context.register(InMemoryMessageQueueTestConfiguration.class);
        context.refresh();

        assertThat(context.containsBean("testQueue")).isTrue();
        assertThat(context.getBean("testQueue", MessageSenderEndpoint.class)).isInstanceOf(DelegateMessageSenderEndpoint.class);
    }

    @Test
    public void shouldRegisterMessageQueueSenderEndpointWithNameDerivedFromChannelName() {
        context.register(SingleUnnamedQueueSenderConfig.class);
        context.register(InMemoryMessageQueueTestConfiguration.class);
        context.refresh();

        assertThat(context.containsBean("testChannelMessageSenderEndpoint")).isTrue();
        final MessageSenderEndpoint senderEndpoint = context.getBean("testChannelMessageSenderEndpoint", MessageSenderEndpoint.class);
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

        assertThat(context.getBean("someChannelMessageSenderEndpoint", MessageSenderEndpoint.class).getChannelName()).isEqualTo("some-channel");
        assertThat(context.getBean("otherChannelMessageSenderEndpoint", MessageSenderEndpoint.class).getChannelName()).isEqualTo("other-channel");
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
        assertThat(first).isInstanceOf(DelegateMessageSenderEndpoint.class);

        final MessageSenderEndpoint second = context.getBean("secondQueue", MessageSenderEndpoint.class);
        assertThat(second.getChannelName()).isEqualTo("second-channel");
        assertThat(second).isInstanceOf(DelegateMessageSenderEndpoint.class);
    }

}
