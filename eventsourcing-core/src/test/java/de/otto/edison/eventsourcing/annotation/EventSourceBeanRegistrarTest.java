package de.otto.edison.eventsourcing.annotation;

import de.otto.edison.eventsourcing.consumer.EventSource;
import de.otto.edison.eventsourcing.testsupport.InMemoryEventSourceConfiguration;
import org.junit.After;
import org.junit.Test;
import org.springframework.beans.factory.BeanCreationException;
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

    @EnableEventSource(name = "testEventSource", streamName = "test-stream", builder = "inMemEventSourceBuilder")
    static class SingleEventSourceTestConfig {
    }

    @EnableEventSource(name = "brokenEventSource", streamName = "some-stream")
    @EnableEventSource(name = "brokenEventSource", streamName = "some-stream")
    static class MultiEventSourceTestConfigWithSameNames {
    }

    @EnableEventSource(name = "firstEventSource", streamName = "some-stream")
    @EnableEventSource(name = "secondEventSource", streamName = "some-stream")
    static class MultiEventSourceTestConfigWithDifferentNames {
    }

    @EnableEventSource(name = "firstEventSource", streamName = "first-stream", builder = "inMemEventSourceBuilder")
    @EnableEventSource(name = "secondEventSource", streamName = "${test.stream-name}")
    static class RepeatableMultiEventSourceTestConfig {
    }

    @EnableEventSources({
            @EnableEventSource(name = "firstEventSource", streamName = "first-stream", builder = "inMemEventSourceBuilder"),
            @EnableEventSource(name = "secondEventSource", streamName = "${test.stream-name}")
    })
    static class MultiEventSourceTestConfig {
    }

    @Test(expected = BeanCreationException.class)
    public void shouldFailToRegisterMultipleEventSourcesForSameStreamNameWithSameName() {
        context.register(MultiEventSourceTestConfigWithSameNames.class);
        context.register(InMemoryEventSourceConfiguration.class);
        context.refresh();
    }

    @Test
    public void shouldRegisterMultipleEventSourcesForSameStreamNameWithDifferentNames() {
        context.register(MultiEventSourceTestConfigWithDifferentNames .class);
        context.register(InMemoryEventSourceConfiguration.class);
        context.refresh();

        assertThat(context.getBean("firstEventSource", EventSource.class).getStreamName()).isEqualTo("some-stream");
        assertThat(context.getBean("secondEventSource", EventSource.class).getStreamName()).isEqualTo("some-stream");

    }

    @Test
    public void shouldRegisterEventSource() {
        context.register(SingleEventSourceTestConfig.class);
        context.register(InMemoryEventSourceConfiguration.class);
        context.refresh();

        assertThat(context.containsBean("testEventSource")).isTrue();
    }

    /* TODO
    @Test
    public void shouldRegisterEventSourceWithDefaultType() {
        context.register(RepeatableMultiEventSourceTestConfig.class);
        context.register(InMemoryEventSourceConfiguration.class);
        context.refresh();

        final DelegateEventSource testEventSource = context.getBean("firstEventSource", DelegateEventSource.class);
        assertThat(testEventSource.getDelegate()).isInstanceOf(CompactingKinesisEventSource.class);
    }

    @Test
    public void shouldRegisterEventSourceWithCustomType() {
        context.register(SingleEventSourceTestConfig.class);
        context.register(InMemoryEventSourceConfiguration.class);
        context.refresh();

        final DelegateEventSource testEventSource = context.getBean("testEventSource", DelegateEventSource.class);
        assertThat(testEventSource.getDelegate()).isInstanceOf(SnapshotEventSource.class);
    }

    @Test
    public void shouldRegisterMultipleEventSources() {
        context.register(RepeatableMultiEventSourceTestConfig.class);
        context.register(EventSourcingConfiguration.class);
        addEnvironment(this.context,
                "test.stream-name=second-stream"
        );
        context.refresh();

        assertThat(context.containsBean("firstEventSource")).isTrue();
        assertThat(context.containsBean("secondEventSource")).isTrue();

        final EventSource first = context.getBean("firstEventSource", DelegateEventSource.class).getDelegate();
        assertThat(first.getStreamName()).isEqualTo("first-stream");
        assertThat(first).isInstanceOf(CompactingKinesisEventSource.class);

        final EventSource second = context.getBean("secondEventSource", DelegateEventSource.class).getDelegate();
        assertThat(second.getStreamName()).isEqualTo("second-stream");
        assertThat(second).isInstanceOf(CompactingKinesisEventSource.class);
    }

    @Test
    public void shouldRegisterMultipleEventSourcesWithEnableEventSources() {
        context.register(MultiEventSourceTestConfig.class);
        context.register(EventSourcingConfiguration.class);
        addEnvironment(this.context,
                "test.stream-name=second-foo-stream"
        );
        context.refresh();

        assertThat(context.containsBean("firstEventSource")).isTrue();
        assertThat(context.containsBean("secondEventSource")).isTrue();

        final EventSource first = context.getBean("firstEventSource", DelegateEventSource.class).getDelegate();
        assertThat(first.getStreamName()).isEqualTo("first-stream");
        assertThat(first).isInstanceOf(CompactingKinesisEventSource.class);

        final EventSource second = context.getBean("secondEventSource", DelegateEventSource.class).getDelegate();
        assertThat(second.getStreamName()).isEqualTo("second-foo-stream");
        assertThat(second).isInstanceOf(CompactingKinesisEventSource.class);
    }
    */
}
