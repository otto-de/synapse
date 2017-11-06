package de.otto.edison.eventsourcing.example.consumer;

import de.otto.edison.eventsourcing.annotation.EventSourceConsumer;
import de.otto.edison.eventsourcing.consumer.Event;
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
            name = "bananaEventConsumer",
            streamName = "${exampleservice.banana-stream-name}",
            payloadType = BananaPayload.class
    )
    public void consumeBananas(final Event<BananaPayload> event) {
        stateRepository.compute(event.key(), (id, bananaProduct) -> {
            final BananaProduct.Builder builder = bananaProduct.isPresent()
                    ? bananaProductBuilder(bananaProduct.get())
                    : bananaProductBuilder();
            return builder
                    .withId(event.key())
                    .withColor(event.payload().getColor())
                    .build();
        });
        LOG.info(stateRepository.toString());
    }

    @EventSourceConsumer(
            name = "productEventConsumer",
            streamName = "${exampleservice.product-stream-name}",
            payloadType = ProductPayload.class
    )
    public void consumeProducts(final Event<ProductPayload> event) {
        stateRepository.compute(event.key(), (s, bananaProduct) -> {
            final BananaProduct.Builder builder = bananaProduct.isPresent()
                    ? bananaProductBuilder(bananaProduct.get())
                    : bananaProductBuilder();
            return builder
                    .withId(event.key())
                    .withPrice(event.payload().getPrice())
                    .build();
        });
        LOG.info(stateRepository.toString());
    }
}
