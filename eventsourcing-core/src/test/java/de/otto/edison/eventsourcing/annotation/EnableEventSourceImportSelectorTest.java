package de.otto.edison.eventsourcing.annotation;

import de.otto.edison.eventsourcing.configuration.EventSourcingConfiguration;
import de.otto.edison.eventsourcing.consumer.EventSource;
import org.junit.After;
import org.junit.Test;
import org.springframework.boot.test.util.EnvironmentTestUtils;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;

public class EnableEventSourceImportSelectorTest {

    private AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();

    @After
    public void close() {
        if (this.context != null) {
            this.context.close();
        }
    }

    @Configuration
    @EnableEventSource(name = "testEventSource", streamName = "test-stream", payloadType = String.class)
    static class SingleEventSourceTestConfig {
    }

    @Configuration
    @EnableEventSource(name = "firstEventSource", streamName = "first-stream", payloadType = String.class)
    @EnableEventSource(name = "secondEventSource", streamName = "${test.stream-name}", payloadType = String.class)
    static class MultiEventSourceTestConfig {
    }

    @Test
    public void shouldRegisterEventSource() {
        context.register(SingleEventSourceTestConfig.class);
        context.register(EventSourcingConfiguration.class);
        context.refresh();

        assertThat(context.containsBean("testEventSource")).isTrue();
        final EventSource testEventSource = context.getBean("testEventSource", EventSource.class);
        assertThat(testEventSource.name()).isEqualTo("test-stream");
    }

    @Test
    public void shouldRegisterMultipleEventSources() {
        context.register(MultiEventSourceTestConfig.class);
        context.register(EventSourcingConfiguration.class);
        EnvironmentTestUtils.addEnvironment(this.context,
                "test.stream-name=second-stream"
        );
        context.refresh();

        assertThat(context.containsBean("firstEventSource")).isTrue();
        assertThat(context.containsBean("secondEventSource")).isTrue();

        final EventSource first = context.getBean("firstEventSource", EventSource.class);
        assertThat(first.name()).isEqualTo("first-stream");

        final EventSource second = context.getBean("secondEventSource", EventSource.class);
        assertThat(second.name()).isEqualTo("second-stream");
    }
}