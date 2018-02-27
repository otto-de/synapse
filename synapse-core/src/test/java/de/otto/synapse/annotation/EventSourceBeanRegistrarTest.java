package de.otto.synapse.annotation;

import de.otto.synapse.configuration.EventSourcingAutoConfiguration;
import de.otto.synapse.eventsource.DelegateEventSource;
import de.otto.synapse.eventsource.EventSource;
import de.otto.synapse.testsupport.InMemoryEventSourceConfiguration;
import de.otto.synapse.testsupport.TestDefaultEventSourceConfiguration;
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

    @EnableEventSource(name = "testEventSource", streamName = "test-stream", builder = "inMemEventSourceBuilder")
    static class SingleEventSourceTestConfig {
    }

    @EnableEventSource(name = "brokenEventSource", streamName = "some-stream", builder = "inMemEventSourceBuilder")
    @EnableEventSource(name = "brokenEventSource", streamName = "some-stream", builder = "inMemEventSourceBuilder")
    static class MultiEventSourceTestConfigWithSameNames {
    }

    @EnableEventSource(name = "firstEventSource", streamName = "some-stream", builder = "inMemEventSourceBuilder")
    @EnableEventSource(name = "secondEventSource", streamName = "some-stream", builder = "inMemEventSourceBuilder")
    static class MultiEventSourceTestConfigWithDifferentNames {
    }

    @EnableEventSource(name = "firstEventSource", streamName = "first-stream")
    @EnableEventSource(name = "secondEventSource", streamName = "${test.stream-name}")
    static class RepeatableMultiEventSourceTestConfig {
    }

    @Test(expected = BeanCreationException.class)
    public void shouldFailToRegisterMultipleEventSourcesForSameStreamNameWithSameName() {
        context.register(EventSourcingAutoConfiguration.class);
        context.register(MultiEventSourceTestConfigWithSameNames.class);
        context.register(InMemoryEventSourceConfiguration.class);
        context.refresh();
    }

    @Test
    public void shouldRegisterMultipleEventSourcesForSameStreamNameWithDifferentNames() {
        context.register(EventSourcingAutoConfiguration.class);
        context.register(MultiEventSourceTestConfigWithDifferentNames .class);
        context.register(InMemoryEventSourceConfiguration.class);
        context.refresh();

        assertThat(context.getBean("firstEventSource", EventSource.class).getStreamName()).isEqualTo("some-stream");
        assertThat(context.getBean("secondEventSource", EventSource.class).getStreamName()).isEqualTo("some-stream");

    }

    @Test
    public void shouldRegisterEventSource() {
        context.register(EventSourcingAutoConfiguration.class);
        context.register(SingleEventSourceTestConfig.class);
        context.register(InMemoryEventSourceConfiguration.class);
        context.refresh();

        assertThat(context.containsBean("testEventSource")).isTrue();
    }

    @Test
    public void shouldRegisterEventSourceWithDefaultType() {
        context.register(EventSourcingAutoConfiguration.class);
        context.register(TestDefaultEventSourceConfiguration.class);
        context.register(RepeatableMultiEventSourceTestConfig.class);
        context.refresh();

        final DelegateEventSource testEventSource = context.getBean("firstEventSource", DelegateEventSource.class);
        assertThat(testEventSource.getDelegate()).isInstanceOf(TestDefaultEventSourceConfiguration.TestEventSource.class);
    }

    @Test
    public void shouldRegisterMultipleEventSources() {
        context.register(RepeatableMultiEventSourceTestConfig.class);
        context.register(EventSourcingAutoConfiguration.class);
        context.register(TestDefaultEventSourceConfiguration.class);
        addEnvironment(this.context,
                "test.stream-name=second-stream"
        );
        context.refresh();

        assertThat(context.containsBean("firstEventSource")).isTrue();
        assertThat(context.containsBean("secondEventSource")).isTrue();

        final EventSource first = context.getBean("firstEventSource", DelegateEventSource.class).getDelegate();
        assertThat(first.getStreamName()).isEqualTo("first-stream");
        assertThat(first).isInstanceOf(TestDefaultEventSourceConfiguration.TestEventSource.class);

        final EventSource second = context.getBean("secondEventSource", DelegateEventSource.class).getDelegate();
        assertThat(second.getStreamName()).isEqualTo("second-stream");
        assertThat(second).isInstanceOf(TestDefaultEventSourceConfiguration.TestEventSource.class);
    }

}
