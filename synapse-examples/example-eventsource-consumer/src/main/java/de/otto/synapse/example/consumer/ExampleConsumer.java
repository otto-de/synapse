package de.otto.synapse.example.consumer;

import de.otto.synapse.annotation.EventSourceConsumer;
import de.otto.synapse.example.consumer.payload.BananaPayload;
import de.otto.synapse.example.consumer.payload.ProductPayload;
import de.otto.synapse.example.consumer.state.BananaProduct;
import de.otto.synapse.message.Message;
import de.otto.synapse.state.StateRepository;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static de.otto.synapse.example.consumer.state.BananaProduct.bananaProductBuilder;
import static org.slf4j.LoggerFactory.getLogger;

@Component
public class ExampleConsumer {

    private static final Logger LOG = getLogger(ExampleConsumer.class);

    private final StateRepository<BananaProduct> stateRepository;

    @Autowired
    public ExampleConsumer(StateRepository<BananaProduct> stateRepository) {
        this.stateRepository = stateRepository;
    }

    @EventSourceConsumer(
            eventSource = "bananaSource",
            payloadType = BananaPayload.class
    )
    public void consumeBananas(final Message<BananaPayload> message) {
        final String entityId = entityIdFrom(message);
        stateRepository.compute(entityId, (id, bananaProduct) -> {
            final BananaProduct.Builder builder = bananaProduct.isPresent()
                    ? bananaProductBuilder(bananaProduct.get())
                    : bananaProductBuilder();
            return builder
                    .withId(entityId)
                    .withColor(message.getPayload().getColor())
                    .build();
        });
        LOG.info("Updated StateRepository using BananaPayload: {}", stateRepository.get(entityId).orElse(null));
    }

    @EventSourceConsumer(
            eventSource = "productSource",
            payloadType = ProductPayload.class
    )
    public void consumeProducts(final Message<ProductPayload> message) {
        final String entityId = entityIdFrom(message);
        stateRepository.compute(entityId, (s, bananaProduct) -> {
            final BananaProduct.Builder builder = bananaProduct.isPresent()
                    ? bananaProductBuilder(bananaProduct.get())
                    : bananaProductBuilder();
            return builder
                    .withId(entityId)
                    .withPrice(message.getPayload().getPrice())
                    .build();
        });
        LOG.info("Updated StateRepository using ProductPayload: {}", stateRepository.get(entityId).orElse(null));
    }

    private String entityIdFrom(final Message<?> message) {
        return message.getKey().partitionKey();
    }
}
