package de.otto.synapse.annotation;

import de.otto.synapse.channel.StartFrom;
import de.otto.synapse.configuration.InMemoryMessageLogTestConfiguration;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpoint;
import de.otto.synapse.eventsource.DefaultEventSource;
import de.otto.synapse.eventsource.DelegateEventSource;
import de.otto.synapse.eventsource.EventSource;
import org.junit.After;
import org.junit.Test;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;

public class EventSourceBeanRegistrarTest {

    private AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();

    @After
    public void close() {
        if (this.context != null) {
            this.context.close();
        }
    }

    @EnableEventSource(name = "testEventSource", channelName = "test-stream")
    static class SingleEventSourceTestConfig {
    }

    @EnableEventSource(name = "testEventSource", channelName = "test-stream", messageLogReceiverEndpoint = "testMessageLog")
    static class SingleEventSourceWithMessageLogTestConfig {
    }

    @EnableEventSource(name = "brokenEventSource", channelName = "some-stream")
    @EnableEventSource(name = "brokenEventSource", channelName = "some-stream")
    static class MultiEventSourceTestConfigWithSameNames {
    }

    @EnableEventSource(name = "firstEventSource", channelName = "some-stream")
    @EnableEventSource(name = "secondEventSource", channelName = "some-stream")
    static class MultiEventSourceTestConfigWithDifferentNames {
    }

    @EnableEventSource(name = "firstEventSource", channelName = "first-stream")
    @EnableEventSource(name = "secondEventSource", channelName = "${test.stream-name}")
    static class RepeatableMultiEventSourceTestConfig {
    }

    @EnableEventSource(name = "testEventSource", channelName = "test-stream", iteratorAt = StartFrom.LATEST)
    static class IteratorAtEventSourceTestConfig {
    }

    @Test(expected = BeanCreationException.class)
    public void shouldFailToRegisterMultipleEventSourcesForSameStreamNameWithSameName() {
        context.register(MultiEventSourceTestConfigWithSameNames.class);
        context.register(InMemoryMessageLogTestConfiguration.class);
        context.refresh();
    }

    @Test(expected = BeanCreationException.class)
    public void shouldFailToRegisterMultipleEventSourcesForSameStream() {
        context.register(MultiEventSourceTestConfigWithDifferentNames .class);
        context.register(InMemoryMessageLogTestConfiguration.class);
        context.refresh();
    }

    @Test
    public void shouldRegisterEventSource() {
        context.register(SingleEventSourceTestConfig.class);
        context.register(InMemoryMessageLogTestConfiguration.class);
        context.refresh();

        assertThat(context.containsBean("testEventSource")).isTrue();
    }

    @Test
    public void shouldRegisterEventSourceWithDefaultType() {
        context.register(InMemoryMessageLogTestConfiguration.class);
        context.register(RepeatableMultiEventSourceTestConfig.class);
        context.refresh();

        final DelegateEventSource testEventSource = context.getBean("firstEventSource", DelegateEventSource.class);
        assertThat(testEventSource.getDelegate()).isInstanceOf(DefaultEventSource.class);
    }

    @Test
    public void shouldRegisterMultipleEventSources() {
        context.register(RepeatableMultiEventSourceTestConfig.class);
        context.register(InMemoryMessageLogTestConfiguration.class);
        TestPropertyValues.of(
                "test.stream-name=second-stream"
        ).applyTo(context);
        context.refresh();

        assertThat(context.containsBean("firstEventSource")).isTrue();
        assertThat(context.containsBean("secondEventSource")).isTrue();

        final EventSource first = context.getBean("firstEventSource", DelegateEventSource.class).getDelegate();
        assertThat(first.getChannelName()).isEqualTo("first-stream");
        assertThat(first).isInstanceOf(DefaultEventSource.class);

        final EventSource second = context.getBean("secondEventSource", DelegateEventSource.class).getDelegate();
        assertThat(second.getChannelName()).isEqualTo("second-stream");
        assertThat(second).isInstanceOf(DefaultEventSource.class);
    }

    @Test
    public void shouldRegisterMessageLogReceiverEndpointWithNameDerivedFromChannelName() {
        context.register(SingleEventSourceTestConfig.class);
        context.register(InMemoryMessageLogTestConfiguration.class);
        context.refresh();

        assertThat(context.containsBean("testStreamMessageLogReceiverEndpoint")).isTrue();
        final MessageLogReceiverEndpoint receiverEndpoint = context.getBean("testStreamMessageLogReceiverEndpoint", MessageLogReceiverEndpoint.class);
        assertThat(receiverEndpoint.getChannelName()).isEqualTo("test-stream");
    }

    @Test
    public void shouldRegisterMessageLogReceiverEndpointWithSpecifiedName() {
        context.register(SingleEventSourceWithMessageLogTestConfig.class);
        context.register(InMemoryMessageLogTestConfiguration.class);
        context.refresh();

        assertThat(context.containsBean("testMessageLog")).isTrue();
        final MessageLogReceiverEndpoint receiverEndpoint = context.getBean("testMessageLog", MessageLogReceiverEndpoint.class);
        assertThat(receiverEndpoint.getChannelName()).isEqualTo("test-stream");
    }

    @Test
    public void shouldRegisterEventSourceWithIteratorAt() {
        context.register(IteratorAtEventSourceTestConfig.class);
        context.register(InMemoryMessageLogTestConfiguration.class);
        context.refresh();

        assertThat(context.containsBean("testEventSource")).isTrue();
        final EventSource eventSource = context.getBean("testEventSource", DelegateEventSource.class).getDelegate();
        assertThat(eventSource.getIterator()).isEqualTo(StartFrom.LATEST);
    }


}
