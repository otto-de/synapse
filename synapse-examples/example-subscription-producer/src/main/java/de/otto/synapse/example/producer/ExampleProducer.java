package de.otto.synapse.example.producer;

import de.otto.synapse.endpoint.sender.MessageSenderEndpoint;
import de.otto.synapse.state.StateRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.PrimitiveIterator;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static de.otto.synapse.message.Message.message;
import static java.lang.String.valueOf;

@Component
public class ExampleProducer {

    private final static Logger LOG = LoggerFactory.getLogger(ExampleProducer.class);

    private final StateRepository<Product> productRepository;
    private final MessageSenderEndpoint messageSender;
    private final AtomicInteger counter = new AtomicInteger();
    private final PrimitiveIterator.OfLong priceGenerator = new Random().longs(0, 100).iterator();

    @Autowired
    public ExampleProducer(final StateRepository<Product> productRepository,
                           final MessageSenderEndpoint productMessageSender) {
        this.messageSender = productMessageSender;
        this.productRepository = productRepository;
    }

    @Scheduled(fixedDelay = 1000L)
    public void produceSampleData() {
        try {
            final Product product = generatePayload();
            productRepository.put(product.getId(), product);
            messageSender.send(message(product.getId(), product));
        } catch (Exception e) {
            LOG.error("error occurred while sending an event", e);
        }
    }

    private Product generatePayload() {
        counter.compareAndSet(50, 0);
        final Product product = new Product();
        product.setId(valueOf(counter.incrementAndGet()));
        product.setPrice(priceGenerator.next());
        return product;
    }

}
