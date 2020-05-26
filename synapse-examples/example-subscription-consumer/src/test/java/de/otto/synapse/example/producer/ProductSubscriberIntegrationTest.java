package de.otto.synapse.example.producer;

import de.otto.synapse.annotation.EnableEventSource;
import de.otto.synapse.annotation.EventSourceConsumer;
import de.otto.synapse.message.Message;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.concurrent.atomic.AtomicInteger;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {
        Server.class,
        ProductSubscriberIntegrationTest.InMemoryTestConsumerConfiguration.class,
        ProductSubscriberIntegrationTest.TestConsumer.class})
public class ProductSubscriberIntegrationTest {

    @Autowired
    ProductSubscriber producer;

    @Autowired
    TestConsumer testConsumer;


    @EnableEventSource(name = "inMemorySource", channelName = "synapse-example-products")
    @Configuration
    static class InMemoryTestConsumerConfiguration {

    }

    @Component
    public static class TestConsumer {

        private AtomicInteger count = new AtomicInteger();

        @EventSourceConsumer(
                eventSource = "inMemorySource",
                payloadType = Product.class
        )
        public void accept(Message<Product> message) {
            count.incrementAndGet();
        }
    }
}
