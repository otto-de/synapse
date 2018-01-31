package de.otto.edison.eventsourcing.example.producer;

import com.jayway.awaitility.Awaitility;
import de.otto.edison.eventsourcing.annotation.EnableEventSource;
import de.otto.edison.eventsourcing.annotation.EventSourceConsumer;
import de.otto.edison.eventsourcing.configuration.EventSourcingConfiguration;
import de.otto.edison.eventsourcing.consumer.Event;
import de.otto.edison.eventsourcing.consumer.EventConsumer;
import de.otto.edison.eventsourcing.consumer.EventSource;
import de.otto.edison.eventsourcing.example.producer.payload.ProductPayload;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {
        Server.class,
        ExampleProducerIntegrationTest.InMemoryTestConsumerConfiguration.class,
        ExampleProducerIntegrationTest.TestConsumer.class})
public class ExampleProducerIntegrationTest {

    @Autowired
    ExampleProducer producer;

    @Autowired
    TestConsumer testConsumer;

    @Autowired
    EventSource productEventSource;

    @Before
    public void setUp() throws Exception {

    }

    @Test
    @Ignore
    public void shouldProduceEvent() throws Exception {
        // given
        producer.produceSampleData();

        // when
        Awaitility.await()
                .atMost(300, SECONDS)
                .untilAtomic(testConsumer.count, is(1));

        //then
    }

    @EnableEventSource(name = "inMemoryStream", streamName = "someStreamName")
    @Configuration
    static class InMemoryTestConsumerConfiguration {

    }

    @Component
    static class TestConsumer {

        private AtomicInteger count = new AtomicInteger();

        @EventSourceConsumer(
                eventSource = "inMemoryStream",
                payloadType = ProductPayload.class
        )
        public void accept(Event<ProductPayload> event) {
            count.incrementAndGet();
        }
    }
}
