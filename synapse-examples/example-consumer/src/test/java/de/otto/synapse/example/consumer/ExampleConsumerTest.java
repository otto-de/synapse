package de.otto.synapse.example.consumer;

import de.otto.synapse.example.consumer.payload.BananaPayload;
import de.otto.synapse.example.consumer.payload.ProductPayload;
import de.otto.synapse.example.consumer.state.BananaProduct;
import de.otto.synapse.message.Message;
import de.otto.synapse.state.ConcurrentHashMapConcurrentStateRepository;
import de.otto.synapse.state.ConcurrentStateRepository;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.util.Optional;

import static de.otto.synapse.channel.ChannelPosition.fromHorizon;
import static de.otto.synapse.message.Header.responseHeader;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ExampleConsumerTest {

    private ExampleConsumer exampleConsumer;
    private ConcurrentStateRepository<BananaProduct> stateRepository;

    @Before
    public void setUp() throws Exception {
        stateRepository = new ConcurrentHashMapConcurrentStateRepository<>();
        exampleConsumer = new ExampleConsumer(stateRepository);
    }

    @Test
    public void shouldCreateBananaPart() {
        // given
        BananaPayload bananaPayload = new BananaPayload();
        bananaPayload.setId("banana_id");
        bananaPayload.setColor("green");

        // when
        exampleConsumer.consumeBananas(testEvent("banana_id", bananaPayload));

        // then
        Optional<BananaProduct> retrievedBanana = stateRepository.get("banana_id");
        assertThat(retrievedBanana.map(BananaProduct::getId), is(Optional.of("banana_id")));
        assertThat(retrievedBanana.map(BananaProduct::getColor), is(Optional.of("green")));
    }

    @Test
    public void shouldCreateProductPart() {
        // given
        ProductPayload productPayload = new ProductPayload();
        productPayload.setId("banana_id");
        productPayload.setPrice(300L);

        // when
        exampleConsumer.consumeProducts(testEvent("banana_id", productPayload));

        // then
        Optional<BananaProduct> retrievedBanana = stateRepository.get("banana_id");
        assertThat(retrievedBanana.map(BananaProduct::getId), is(Optional.of("banana_id")));
        assertThat(retrievedBanana.map(BananaProduct::getPrice), is(Optional.of(300L)));
    }

    @Test
    public void shouldMergeDataFromBothStreams() {
        // given
        ProductPayload productPayload = new ProductPayload();
        productPayload.setId("banana_id");
        productPayload.setPrice(300L);
        BananaPayload bananaPayload = new BananaPayload();
        bananaPayload.setId("banana_id");
        bananaPayload.setColor("green");

        // when
        exampleConsumer.consumeProducts(testEvent("banana_id", productPayload));
        exampleConsumer.consumeBananas(testEvent("banana_id", bananaPayload));

        // then
        Optional<BananaProduct> retrievedBanana = stateRepository.get("banana_id");
        assertThat(retrievedBanana.map(BananaProduct::getId), is(Optional.of("banana_id")));
        assertThat(retrievedBanana.map(BananaProduct::getColor), is(Optional.of("green")));
        assertThat(retrievedBanana.map(BananaProduct::getPrice), is(Optional.of(300L)));
    }

    private <T> Message<T> testEvent(String key, T payload) {
        return Message.message(
                key,
                responseHeader(fromHorizon(), Instant.now(), null),
                payload
        );
    }
}
