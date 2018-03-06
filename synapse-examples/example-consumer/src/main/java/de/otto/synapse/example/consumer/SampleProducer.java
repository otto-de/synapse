package de.otto.synapse.example.consumer;

import de.otto.synapse.endpoint.MessageSenderEndpoint;
import de.otto.synapse.example.consumer.configuration.MyServiceProperties;
import de.otto.synapse.example.consumer.payload.BananaPayload;
import de.otto.synapse.example.consumer.payload.ProductPayload;
import de.otto.synapse.message.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;

import static java.util.Arrays.asList;

@Component
@EnableConfigurationProperties(MyServiceProperties.class)
@Profile("!test")
public class SampleProducer {

    @Autowired
    private MessageSenderEndpoint bananaMessageSender;
    @Autowired
    private MessageSenderEndpoint productMessageSender;

    @PostConstruct
    public void produceSampleData() {
        produceBananaSampleData();
        produceProductsSampleData();
    }

    protected void produceBananaSampleData() {
        final List<Message<BananaPayload>> bananaMessages = asList(
                sampleBananaMessage("1"),
                sampleBananaMessage("2"),
                sampleBananaMessage("3"),
                sampleBananaMessage("4"),
                sampleBananaMessage("5"),
                sampleBananaMessage("6")
        );
        bananaMessageSender.sendBatch(bananaMessages.stream());
    }

    protected void produceProductsSampleData() {
        final List<Message<ProductPayload>> productMessages = asList(
                sampleProductMessage("1"),
                sampleProductMessage("2"),
                sampleProductMessage("3"),
                sampleProductMessage("4"),
                sampleProductMessage("5"),
                sampleProductMessage("6")
                );
        productMessageSender.sendBatch(productMessages.stream());
    }

    private Message<BananaPayload> sampleBananaMessage(final String id) {
        final BananaPayload bananaPayload = new BananaPayload();
        bananaPayload.setId(id);
        bananaPayload.setColor("red");
        return Message.message(id, bananaPayload);
    }

    private Message<ProductPayload> sampleProductMessage(final String id) {
        final ProductPayload productPayload = new ProductPayload();
        productPayload.setId(id);
        productPayload.setPrice(123L);
        return Message.message(id, productPayload);
    }


}
