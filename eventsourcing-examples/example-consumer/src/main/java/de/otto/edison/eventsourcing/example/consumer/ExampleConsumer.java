package de.otto.edison.eventsourcing.example.consumer;

import de.otto.edison.eventsourcing.annotation.EventSourceConsumer;
import de.otto.edison.eventsourcing.event.Message;
import de.otto.edison.eventsourcing.example.consumer.payload.BananaPayload;
import de.otto.edison.eventsourcing.example.consumer.payload.ProductPayload;
import de.otto.edison.eventsourcing.example.consumer.state.BananaProduct;
import de.otto.edison.eventsourcing.state.StateRepository;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static de.otto.edison.eventsourcing.example.consumer.state.BananaProduct.bananaProductBuilder;
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
        stateRepository.compute(message.getKey(), (id, bananaProduct) -> {
            final BananaProduct.Builder builder = bananaProduct.isPresent()
                    ? bananaProductBuilder(bananaProduct.get())
                    : bananaProductBuilder();
            return builder
                    .withId(message.getKey())
                    .withColor(message.getPayload().getColor())
                    .build();
        });
        LOG.info(stateRepository.toString());
    }

    @EventSourceConsumer(
            eventSource = "productSource",
            payloadType = ProductPayload.class
    )
    public void consumeProducts(final Message<ProductPayload> message) {
        stateRepository.compute(message.getKey(), (s, bananaProduct) -> {
            final BananaProduct.Builder builder = bananaProduct.isPresent()
                    ? bananaProductBuilder(bananaProduct.get())
                    : bananaProductBuilder();
            return builder
                    .withId(message.getKey())
                    .withPrice(message.getPayload().getPrice())
                    .build();
        });
        LOG.info(stateRepository.toString());
    }
}
