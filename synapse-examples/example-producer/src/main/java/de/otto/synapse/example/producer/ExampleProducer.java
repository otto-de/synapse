package de.otto.synapse.example.producer;

import de.otto.synapse.example.producer.configuration.MyServiceProperties;
import de.otto.synapse.example.producer.payload.ProductPayload;
import de.otto.synapse.sender.MessageSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;

import static de.otto.synapse.message.Message.message;

@Component
@EnableConfigurationProperties(MyServiceProperties.class)
public class ExampleProducer {

    private final static Logger LOG = LoggerFactory.getLogger(ExampleProducer.class);

    private MessageSender messageSender;

    @Autowired
    public ExampleProducer(MessageSender productMessageSender) {
        this.messageSender = productMessageSender;
    }

    @Scheduled(fixedDelay = 3000L)
    public void produceSampleData() {
        try {
            ProductPayload productPayload = generatePayload();
            messageSender.send(message(productPayload.getId(), productPayload));
        } catch (Exception e) {
            LOG.error("error occurred while sending an event", e);
        }
    }

    private ProductPayload generatePayload() {
        ProductPayload productPayload = new ProductPayload();
        productPayload.setId("id_" + Instant.now().getEpochSecond());
        productPayload.setPrice(1234L);
        return productPayload;
    }

}
