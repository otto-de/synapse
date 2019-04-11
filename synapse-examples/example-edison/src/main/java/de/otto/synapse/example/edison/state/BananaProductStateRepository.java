package de.otto.synapse.example.edison.state;

import de.otto.synapse.annotation.EventSourceConsumer;
import de.otto.synapse.example.edison.payload.BananaPayload;
import de.otto.synapse.example.edison.payload.ProductPayload;
import de.otto.synapse.journal.JournalingStateRepository;
import de.otto.synapse.message.Message;
import de.otto.synapse.messagestore.MessageStore;
import de.otto.synapse.state.StateRepository;
import org.slf4j.Logger;

import java.util.Optional;

import static de.otto.synapse.example.edison.state.BananaProduct.bananaProductBuilder;
import static org.slf4j.LoggerFactory.getLogger;

public class BananaProductStateRepository extends JournalingStateRepository<BananaProduct> {

    private static final Logger LOG = getLogger(BananaProductStateRepository.class);

    public BananaProductStateRepository(final StateRepository<BananaProduct> delegate,
                                        final MessageStore journalMessageStore) {
        super(delegate, journalMessageStore);
    }

    @EventSourceConsumer(
            eventSource = "bananaSource",
            payloadType = BananaPayload.class
    )
    public void consumeBananas(final Message<BananaPayload> message) {
        final String entityId = message.getKey().partitionKey();

        final Optional<BananaProduct> computed = compute(entityId, (s, bananaProduct) -> {
            final BananaProduct.Builder builder = bananaProduct.isPresent()
                    ? bananaProductBuilder(bananaProduct.get())
                    : bananaProductBuilder();
            final BananaProduct product = builder
                    .withId(entityId)
                    .withColor(message.getPayload().getColor())
                    .build();
            return product;
        });
        LOG.info("Updated StateRepository using bananaPayload: {}", computed.orElse(null));
    }

    @EventSourceConsumer(
            eventSource = "productSource",
            payloadType = ProductPayload.class
    )
    public void consumeProducts(final Message<ProductPayload> message) {
        final String entityId = message.getKey().partitionKey();
        final Optional<BananaProduct> computed = compute(entityId, (s, bananaProduct) -> {
            final BananaProduct.Builder builder = bananaProduct.isPresent()
                    ? bananaProductBuilder(bananaProduct.get())
                    : bananaProductBuilder();
            return builder
                    .withId(entityId)
                    .withPrice(message.getPayload().getPrice())
                    .build();
        });
        LOG.info("Updated StateRepository using ProductPayload: {}", computed.orElse(null));
    }

}
