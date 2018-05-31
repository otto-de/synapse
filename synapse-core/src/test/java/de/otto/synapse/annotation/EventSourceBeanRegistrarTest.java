package de.otto.synapse.annotation;

import de.otto.synapse.configuration.InMemoryTestConfiguration;
import de.otto.synapse.configuration.SynapseAutoConfiguration;
import de.otto.synapse.eventsource.DefaultEventSource;
import de.otto.synapse.eventsource.DelegateEventSource;
import de.otto.synapse.eventsource.EventSource;
import org.junit.After;
import org.junit.Test;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.boot.test.util.EnvironmentTestUtils.addEnvironment;

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

    @Test(expected = BeanCreationException.class)
    public void shouldFailToRegisterMultipleEventSourcesForSameStreamNameWithSameName() {
        context.register(SynapseAutoConfiguration.class);
        context.register(MultiEventSourceTestConfigWithSameNames.class);
        context.register(InMemoryTestConfiguration.class);
        context.refresh();
    }

    @Test
    public void shouldRegisterMultipleEventSourcesForSameStreamNameWithDifferentNames() {
        context.register(SynapseAutoConfiguration.class);
        context.register(MultiEventSourceTestConfigWithDifferentNames .class);
        context.register(InMemoryTestConfiguration.class);
        context.refresh();

        assertThat(context.getBean("firstEventSource", EventSource.class).getChannelName()).isEqualTo("some-stream");
        assertThat(context.getBean("secondEventSource", EventSource.class).getChannelName()).isEqualTo("some-stream");

    }

    @Test
    public void shouldRegisterEventSource() {
        context.register(SynapseAutoConfiguration.class);
        context.register(SingleEventSourceTestConfig.class);
        context.register(InMemoryTestConfiguration.class);
        context.refresh();

        assertThat(context.containsBean("testEventSource")).isTrue();
    }

    @Test
    public void shouldRegisterEventSourceWithDefaultType() {
        context.register(SynapseAutoConfiguration.class);
        context.register(InMemoryTestConfiguration.class);
        context.register(RepeatableMultiEventSourceTestConfig.class);
        context.refresh();

        final DelegateEventSource testEventSource = context.getBean("firstEventSource", DelegateEventSource.class);
        assertThat(testEventSource.getDelegate()).isInstanceOf(DefaultEventSource.class);
    }

    @Test
    public void shouldRegisterMultipleEventSources() {
        context.register(RepeatableMultiEventSourceTestConfig.class);
        context.register(InMemoryTestConfiguration.class);
        context.register(SynapseAutoConfiguration.class);
        addEnvironment(this.context,
                "test.stream-name=second-stream"
        );
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

}
