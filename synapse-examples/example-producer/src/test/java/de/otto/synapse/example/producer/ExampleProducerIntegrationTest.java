package de.otto.synapse.example.producer;

import com.jayway.awaitility.Awaitility;
import de.otto.synapse.annotation.EnableEventSource;
import de.otto.synapse.annotation.EventSourceConsumer;
import de.otto.synapse.example.producer.payload.ProductPayload;
import de.otto.synapse.message.Message;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

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

    @Test
    public void shouldProduceEvent() throws Exception {
        // when
        producer.produceSampleData();

        //then
        Awaitility.await()
                .atMost(3, SECONDS)
                .untilAtomic(testConsumer.count, greaterThanOrEqualTo(1));
    }

    @EnableEventSource(name = "inMemorySource", channelName = "synapse-example-products")
    @Configuration
    static class InMemoryTestConsumerConfiguration {

    }

    @Component
    public static class TestConsumer {

        private AtomicInteger count = new AtomicInteger();

        @EventSourceConsumer(
                eventSource = "inMemorySource",
                payloadType = ProductPayload.class
        )
        public void accept(Message<ProductPayload> message) {
            count.incrementAndGet();
        }
    }
}
