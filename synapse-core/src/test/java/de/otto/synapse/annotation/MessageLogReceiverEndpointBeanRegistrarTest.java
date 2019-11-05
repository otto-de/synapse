package de.otto.synapse.annotation;

import de.otto.synapse.configuration.InMemoryMessageLogTestConfiguration;
import de.otto.synapse.endpoint.receiver.DelegateMessageLogReceiverEndpoint;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpoint;
import org.junit.After;
import org.junit.Test;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;

public class MessageLogReceiverEndpointBeanRegistrarTest {

    private AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();

    @After
    public void close() {
        if (this.context != null) {
            this.context.close();
        }
    }

    @EnableMessageLogReceiverEndpoint(name = "testLog", channelName = "test-channel")
    private static class SingleLogReceiverConfig {
    }

    @EnableMessageLogReceiverEndpoint(channelName = "test-channel")
    private static class SingleUnnamedLogReceiverConfig {
    }

    @EnableMessageLogReceiverEndpoint(name = "broken", channelName = "some-channel")
    @EnableMessageLogReceiverEndpoint(name = "broken", channelName = "some-channel")
    private static class RepeatableLogReceiverConfigWithSameNames {
    }

    @EnableMessageLogReceiverEndpoint(channelName = "some-channel")
    @EnableMessageLogReceiverEndpoint(channelName = "other-channel")
    private static class RepeatableUnnamedLogReceiverConfigWithDifferentChannels {
    }

    @EnableMessageLogReceiverEndpoint(name = "first-channel", channelName = "first-channel")
    @EnableMessageLogReceiverEndpoint(name = "second-channel", channelName = "${test.channel-name}")
    private static class RepeatableLogReceiverConfig {
    }

    @Test
    public void shouldRegisterMessageLogReceiverEndpointBean() {
        context.register(SingleLogReceiverConfig.class);
        context.register(InMemoryMessageLogTestConfiguration.class);
        context.refresh();

        assertThat(context.containsBean("testLog")).isTrue();
    }

    @Test
    public void shouldRegisterMessageLogReceiverEndpointWithNameDerivedFromChannelName() {
        context.register(SingleUnnamedLogReceiverConfig.class);
        context.register(InMemoryMessageLogTestConfiguration.class);
        context.refresh();

        assertThat(context.containsBean("testChannelMessageLogReceiverEndpoint")).isTrue();
        final MessageLogReceiverEndpoint receiverEndpoint = context.getBean("testChannelMessageLogReceiverEndpoint", MessageLogReceiverEndpoint.class);
        assertThat(receiverEndpoint.getChannelName()).isEqualTo("test-channel");
    }

    @Test(expected = BeanCreationException.class)
    public void shouldFailToRegisterMultipleLogsForSameChannelNameWithSameName() {
        context.register(RepeatableLogReceiverConfigWithSameNames.class);
        context.register(InMemoryMessageLogTestConfiguration.class);
        context.refresh();
    }

    @Test
    public void shouldRegisterMultipleUnnamedLogsForDifferentChannels() {
        context.register(RepeatableUnnamedLogReceiverConfigWithDifferentChannels.class);
        context.register(InMemoryMessageLogTestConfiguration.class);
        context.refresh();

        assertThat(context.getBean("someChannelMessageLogReceiverEndpoint", MessageLogReceiverEndpoint.class).getChannelName()).isEqualTo("some-channel");
        assertThat(context.getBean("otherChannelMessageLogReceiverEndpoint", MessageLogReceiverEndpoint.class).getChannelName()).isEqualTo("other-channel");
    }

    @Test
    public void shouldRegisterMultipleLogs() {
        context.register(RepeatableLogReceiverConfig.class);
        context.register(InMemoryMessageLogTestConfiguration.class);
        TestPropertyValues.of(
                "test.channel-name=second-channel"
        ).applyTo(context);
        context.refresh();

        assertThat(context.containsBean("first-channel")).isTrue();
        assertThat(context.containsBean("second-channel")).isTrue();

        final MessageLogReceiverEndpoint first = context.getBean("first-channel", MessageLogReceiverEndpoint.class);
        assertThat(first.getChannelName()).isEqualTo("first-channel");
        assertThat(first).isInstanceOf(DelegateMessageLogReceiverEndpoint.class);

        final MessageLogReceiverEndpoint second = context.getBean("second-channel", MessageLogReceiverEndpoint.class);
        assertThat(second.getChannelName()).isEqualTo("second-channel");
        assertThat(second).isInstanceOf(DelegateMessageLogReceiverEndpoint.class);
    }

}
