package de.otto.synapse.annotation;

import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.InMemoryChannel;
import de.otto.synapse.channel.ShardResponse;
import de.otto.synapse.channel.selector.MessageLog;
import de.otto.synapse.channel.selector.Selector;
import de.otto.synapse.configuration.InMemoryMessageLogTestConfiguration;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.receiver.AbstractMessageLogReceiverEndpoint;
import de.otto.synapse.endpoint.receiver.DelegateMessageLogReceiverEndpoint;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpoint;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpointFactory;
import org.junit.After;
import org.junit.Test;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

import static de.otto.synapse.channel.ChannelPosition.fromHorizon;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

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

    interface CustomSelector extends MessageLog {}
    static class CustomMessageLogReceiverEndpoint extends AbstractMessageLogReceiverEndpoint {

        public CustomMessageLogReceiverEndpoint(@Nonnull String channelName,
                                                @Nonnull MessageInterceptorRegistry interceptorRegistry,
                                                @Nullable ApplicationEventPublisher eventPublisher) {
            super(channelName, interceptorRegistry, eventPublisher);
        }

        @Nonnull
        @Override
        public CompletableFuture<ChannelPosition> consumeUntil(@Nonnull ChannelPosition startFrom, @Nonnull Predicate<ShardResponse> stopCondition) {
            return completedFuture(fromHorizon());
        }

        @Override
        public void stop() {
        }
    }
    static class CustomMessageLogReceiverEndpointFactory implements MessageLogReceiverEndpointFactory {

        @Override
        public MessageLogReceiverEndpoint create(@Nonnull String channelName) {
            return new CustomMessageLogReceiverEndpoint(channelName, new MessageInterceptorRegistry(), mock(ApplicationEventPublisher.class));
        }

        @Override
        public boolean matches(Class<? extends Selector> channelSelector) {
            return channelSelector.isAssignableFrom(selector());
        }

        @Override
        public Class<? extends Selector> selector() {
            return CustomSelector.class;
        }
    }

    @EnableMessageLogReceiverEndpoint(name = "first-channel", channelName = "first-channel")
    @EnableMessageLogReceiverEndpoint(name = "second-channel", channelName = "second-channel", selector = CustomSelector.class)
    private static class SelectingLogReceiverConfig {

        @Bean
        MessageLogReceiverEndpointFactory customMessageLogReceiverEndpointFactory() {
            return new CustomMessageLogReceiverEndpointFactory();
        }
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

    @Test
    public void shouldRegisterDifferentImplementationsUsingSelectors() {
        context.register(SelectingLogReceiverConfig.class);
        context.register(InMemoryMessageLogTestConfiguration.class);
        context.refresh();

        assertThat(context.containsBean("first-channel")).isTrue();
        assertThat(context.containsBean("second-channel")).isTrue();

        final MessageLogReceiverEndpoint first = context.getBean("first-channel", MessageLogReceiverEndpoint.class);
        assertThat(first.getChannelName()).isEqualTo("first-channel");
        assertThat(first).isInstanceOf(DelegateMessageLogReceiverEndpoint.class);
        assertThat(((DelegateMessageLogReceiverEndpoint)first).getDelegate()).isInstanceOf(InMemoryChannel.class);

        MessageLogReceiverEndpointFactory endpointFactory = context.getBean("customMessageLogReceiverEndpointFactory", MessageLogReceiverEndpointFactory.class);
        assertThat(endpointFactory).isInstanceOf(CustomMessageLogReceiverEndpointFactory.class);

        final MessageLogReceiverEndpoint second = context.getBean("second-channel", MessageLogReceiverEndpoint.class);
        assertThat(second.getChannelName()).isEqualTo("second-channel");
        assertThat(second).isInstanceOf(DelegateMessageLogReceiverEndpoint.class);
        assertThat(((DelegateMessageLogReceiverEndpoint)second).getDelegate()).isInstanceOf(CustomMessageLogReceiverEndpoint.class);
    }

}
