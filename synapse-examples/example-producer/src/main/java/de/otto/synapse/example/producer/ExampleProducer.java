package de.otto.synapse.example.producer;

import de.otto.synapse.endpoint.sender.MessageSender;
import de.otto.synapse.example.producer.configuration.MyServiceProperties;
import de.otto.synapse.example.producer.payload.ProductPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;

import static de.otto.synapse.message.Message.message;
import static java.lang.String.valueOf;

@Component
@EnableConfigurationProperties(MyServiceProperties.class)
public class ExampleProducer {

    private final static Logger LOG = LoggerFactory.getLogger(ExampleProducer.class);

    private final MessageSender messageSender;
    private final AtomicInteger counter = new AtomicInteger();

    @Autowired
    public ExampleProducer(final MessageSender productMessageSender) {
        this.messageSender = productMessageSender;
    }

    @Scheduled(fixedDelay = 3000L)
    public void produceSampleData() {
        try {
            final ProductPayload productPayload = generatePayload();
            messageSender.send(message(productPayload.getId(), productPayload));
        } catch (Exception e) {
            LOG.error("error occurred while sending an event", e);
        }
    }

    private ProductPayload generatePayload() {
        final ProductPayload productPayload = new ProductPayload();
        productPayload.setId(valueOf(counter.incrementAndGet()));
        productPayload.setPrice(1234L);
        return productPayload;
    }

}
