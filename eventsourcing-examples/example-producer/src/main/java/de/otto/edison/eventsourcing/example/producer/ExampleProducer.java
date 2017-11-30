package de.otto.edison.eventsourcing.example.producer;

import de.otto.edison.eventsourcing.example.producer.configuration.MyServiceProperties;
import de.otto.edison.eventsourcing.example.producer.payload.ProductPayload;
import de.otto.edison.eventsourcing.kinesis.KinesisEventSender;
import de.otto.edison.eventsourcing.kinesis.KinesisEventSenderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
@EnableConfigurationProperties(MyServiceProperties.class)
public class ExampleProducer {

    private final static Logger LOG = LoggerFactory.getLogger(ExampleProducer.class);

    private KinesisEventSender kinesisEventSender;

    @Autowired
    public ExampleProducer(KinesisEventSenderFactory kinesisEventSenderFactory, MyServiceProperties properties) {
        kinesisEventSender = kinesisEventSenderFactory.createSenderForStream(properties.getProductStreamName());
    }

    @Scheduled(fixedDelay = 3000L)
    private void produceSampleData() {
        try {
            ProductPayload productPayload = generatePayload();
            kinesisEventSender.sendEvent(productPayload.getId(), productPayload);
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
