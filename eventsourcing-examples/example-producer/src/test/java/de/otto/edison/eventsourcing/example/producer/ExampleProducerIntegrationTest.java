package de.otto.edison.eventsourcing.example.producer;

import com.jayway.awaitility.Awaitility;
import de.otto.edison.eventsourcing.annotation.EnableEventSource;
import de.otto.edison.eventsourcing.annotation.EventSourceConsumer;
import de.otto.edison.eventsourcing.event.Event;
import de.otto.edison.eventsourcing.consumer.EventSource;
import de.otto.edison.eventsourcing.example.producer.payload.ProductPayload;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
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
    public void shouldProduceEvent() throws Exception {
        // when
        producer.produceSampleData();

        //then
        Awaitility.await()
                .atMost(3, SECONDS)
                .untilAtomic(testConsumer.count, greaterThanOrEqualTo(1));
    }

    @EnableEventSource(name = "inMemoryStream", streamName = "someStreamName")
    @Configuration
    static class InMemoryTestConsumerConfiguration {

    }

    @Component
    public static class TestConsumer {

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
