package de.otto.synapse.annotation;

import de.otto.synapse.channel.selector.MessageLog;
import de.otto.synapse.channel.selector.MessageQueue;
import de.otto.synapse.configuration.InMemoryMessageLogTestConfiguration;
import de.otto.synapse.configuration.InMemoryMessageQueueTestConfiguration;
import de.otto.synapse.endpoint.sender.DelegateMessageSenderEndpoint;
import de.otto.synapse.endpoint.sender.MessageSenderEndpoint;
import de.otto.synapse.translator.MessageFormat;
import org.junit.After;
import org.junit.Test;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;

public class MessageSenderEndpointBeanRegistrarTest {

    private AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();

    @After
    public void close() {
        if (this.context != null) {
            this.context.close();
        }
    }

    @EnableMessageSenderEndpoint(name = "testQueue", channelName = "test-channel", selector = MessageQueue.class)
    private static class SingleQueueSenderConfig {
    }

    @EnableMessageSenderEndpoint(channelName = "test-channel", selector = MessageQueue.class)
    private static class SingleUnnamedQueueSenderConfig {
    }

    @EnableMessageSenderEndpoint(name = "broken", channelName = "some-channel", selector = MessageQueue.class)
    @EnableMessageSenderEndpoint(name = "broken", channelName = "some-channel", selector = MessageQueue.class)
    private static class RepeatableQueueSenderConfigWithSameNames {
    }

    @EnableMessageSenderEndpoint(channelName = "some-channel", selector = MessageQueue.class)
    @EnableMessageSenderEndpoint(channelName = "other-channel", selector = MessageQueue.class)
    private static class RepeatableUnnamedQueueSenderConfigWithDifferentChannels {
    }

    @EnableMessageSenderEndpoint(name = "firstQueue", channelName = "first-channel", selector = MessageQueue.class)
    @EnableMessageSenderEndpoint(name = "secondQueue", channelName = "${test.channel-name}", selector = MessageQueue.class)
    private static class RepeatableQueueSenderConfig {
    }

    @EnableMessageSenderEndpoint(name = "testLog", channelName = "test-channel", selector = MessageLog.class)
    private static class SingleLogSenderConfig {
    }

    @EnableMessageSenderEndpoint(channelName = "test-channel", selector = MessageLog.class)
    private static class SingleUnnamedLogSenderConfig {
    }

    @EnableMessageSenderEndpoint(name = "test-message-format", channelName = "test-message-format-channel", selector = MessageLog.class, messageFormat = MessageFormat.V2)
    private static class MessageFormatLogSenderConfig {
    }

    @EnableMessageSenderEndpoint(name = "repeatable-test-message-format-1", channelName = "test-message-format-channel", selector = MessageLog.class, messageFormat = MessageFormat.V1)
    @EnableMessageSenderEndpoint(name = "repeatable-test-message-format-2", channelName = "test-message-format-channel", selector = MessageLog.class, messageFormat = MessageFormat.V2)
    private static class RepeatableMessageFormatLogSenderConfig {
    }

    @EnableMessageSenderEndpoint(name = "broken", channelName = "some-channel", selector = MessageLog.class)
    @EnableMessageSenderEndpoint(name = "broken", channelName = "some-channel", selector = MessageLog.class)
    private static class RepeatableLogSenderConfigWithSameNames {
    }

    @EnableMessageSenderEndpoint(channelName = "some-channel", selector = MessageLog.class)
    @EnableMessageSenderEndpoint(channelName = "other-channel", selector = MessageLog.class)
    private static class RepeatableUnnamedLogSenderConfigWithDifferentChannels {
    }

    @EnableMessageSenderEndpoint(name = "firstLog", channelName = "first-channel", selector = MessageLog.class)
    @EnableMessageSenderEndpoint(name = "secondLog", channelName = "${test.channel-name}", selector = MessageLog.class)
    private static class RepeatableLogSenderConfig {
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
        TestPropertyValues.of(
                "test.channel-name=second-channel"
        ).applyTo(context);
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

    @Test
    public void shouldRegisterMessageLogSenderEndpointBean() {
        context.register(SingleLogSenderConfig.class);
        context.register(InMemoryMessageLogTestConfiguration.class);
        context.refresh();

        assertThat(context.containsBean("testLog")).isTrue();
        assertThat(context.getBean("testLog", MessageSenderEndpoint.class)).isInstanceOf(DelegateMessageSenderEndpoint.class);
    }

    @Test
    public void shouldRegisterMessageLogSenderEndpointBeanWithMessageFormat() {
        context.register(MessageFormatLogSenderConfig.class);
        context.register(InMemoryMessageLogTestConfiguration.class);
        context.refresh();

        assertThat(context.containsBean("test-message-format")).isTrue();
        assertThat(context.getBean("test-message-format", MessageSenderEndpoint.class)).isInstanceOf(DelegateMessageSenderEndpoint.class);

        DelegateMessageSenderEndpoint bean = context.getBean("test-message-format", DelegateMessageSenderEndpoint.class);
        assertThat(bean.getMessageFormat()).isEqualTo(MessageFormat.V2);
    }


    @Test
    public void shouldRegisterMessageLogSenderEndpointBeansWithMessageFormat() {
        context.register(RepeatableMessageFormatLogSenderConfig.class);
        context.register(InMemoryMessageLogTestConfiguration.class);
        context.refresh();

        assertThat(context.containsBean("repeatable-test-message-format-1")).isTrue();
        assertThat(context.containsBean("repeatable-test-message-format-2")).isTrue();

        assertThat(context.getBean("repeatable-test-message-format-1", MessageSenderEndpoint.class)).isInstanceOf(DelegateMessageSenderEndpoint.class);
        assertThat(context.getBean("repeatable-test-message-format-2", MessageSenderEndpoint.class)).isInstanceOf(DelegateMessageSenderEndpoint.class);

        DelegateMessageSenderEndpoint endpoint1 = context.getBean("repeatable-test-message-format-1", DelegateMessageSenderEndpoint.class);
        assertThat(endpoint1.getMessageFormat()).isEqualTo(MessageFormat.V1);

        DelegateMessageSenderEndpoint endpoint2 = context.getBean("repeatable-test-message-format-2", DelegateMessageSenderEndpoint.class);
        assertThat(endpoint2.getMessageFormat()).isEqualTo(MessageFormat.V2);
    }

    @Test
    public void shouldRegisterMessageLogSenderEndpointWithNameDerivedFromChannelName() {
        context.register(SingleUnnamedLogSenderConfig.class);
        context.register(InMemoryMessageLogTestConfiguration.class);
        context.refresh();

        assertThat(context.containsBean("testChannelMessageSenderEndpoint")).isTrue();
        final MessageSenderEndpoint senderEndpoint = context.getBean("testChannelMessageSenderEndpoint", MessageSenderEndpoint.class);
        assertThat(senderEndpoint.getChannelName()).isEqualTo("test-channel");
    }

    @Test(expected = BeanCreationException.class)
    public void shouldFailToRegisterMultipleLogSendersForSameChannelNameWithSameName() {
        context.register(RepeatableLogSenderConfigWithSameNames.class);
        context.register(InMemoryMessageLogTestConfiguration.class);
        context.refresh();
    }

    @Test
    public void shouldRegisterMultipleUnnamedLogSendersForDifferentChannels() {
        context.register(RepeatableUnnamedLogSenderConfigWithDifferentChannels.class);
        context.register(InMemoryMessageLogTestConfiguration.class);
        context.refresh();

        assertThat(context.getBean("someChannelMessageSenderEndpoint", MessageSenderEndpoint.class).getChannelName()).isEqualTo("some-channel");
        assertThat(context.getBean("otherChannelMessageSenderEndpoint", MessageSenderEndpoint.class).getChannelName()).isEqualTo("other-channel");
    }

    @Test
    public void shouldRegisterMultipleLogSenders() {
        context.register(RepeatableLogSenderConfig.class);
        context.register(InMemoryMessageLogTestConfiguration.class);
        TestPropertyValues.of(
                "test.channel-name=second-channel"
        ).applyTo(context);
        context.refresh();

        assertThat(context.containsBean("firstLog")).isTrue();
        assertThat(context.containsBean("secondLog")).isTrue();

        final MessageSenderEndpoint first = context.getBean("firstLog", MessageSenderEndpoint.class);
        assertThat(first.getChannelName()).isEqualTo("first-channel");
        assertThat(first).isInstanceOf(DelegateMessageSenderEndpoint.class);

        final MessageSenderEndpoint second = context.getBean("secondLog", MessageSenderEndpoint.class);
        assertThat(second.getChannelName()).isEqualTo("second-channel");
        assertThat(second).isInstanceOf(DelegateMessageSenderEndpoint.class);
    }
}
