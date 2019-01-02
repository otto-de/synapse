package de.otto.synapse.example.edison;

import de.otto.synapse.channel.ShardPosition;
import de.otto.synapse.example.edison.payload.BananaPayload;
import de.otto.synapse.example.edison.payload.ProductPayload;
import de.otto.synapse.example.edison.state.BananaProduct;
import de.otto.synapse.message.Message;
import de.otto.synapse.state.ConcurrentHashMapStateRepository;
import de.otto.synapse.state.StateRepository;
import org.junit.Before;
import org.junit.Test;

import java.util.Optional;

import static de.otto.synapse.message.Header.of;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class ExampleConsumerTest {

    private ExampleConsumer exampleConsumer;
    private StateRepository<BananaProduct> stateRepository;

    @Before
    public void setUp() throws Exception {
        stateRepository = new ConcurrentHashMapStateRepository<>();
        exampleConsumer = new ExampleConsumer(stateRepository);
    }

    @Test
    public void shouldCreateBananaPart() {
        // given
        BananaPayload bananaPayload = new BananaPayload();
        bananaPayload.setId("banana_id");
        bananaPayload.setColor("green");

        // when
        exampleConsumer.consumeBananas(testMessage("banana_id", bananaPayload));

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
        exampleConsumer.consumeProducts(testMessage("banana_id", productPayload));

        // then
        Optional<BananaProduct> retrievedBanana = stateRepository.get("banana_id");
        assertThat(retrievedBanana.map(BananaProduct::getId).map(Object::toString), is(Optional.of("banana_id")));
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
        exampleConsumer.consumeProducts(testMessage("banana_id", productPayload));
        exampleConsumer.consumeBananas(testMessage("banana_id", bananaPayload));

        // then
        Optional<BananaProduct> retrievedBanana = stateRepository.get("banana_id");
        assertThat(retrievedBanana.map(BananaProduct::getId), is(Optional.of("banana_id")));
        assertThat(retrievedBanana.map(BananaProduct::getColor), is(Optional.of("green")));
        assertThat(retrievedBanana.map(BananaProduct::getPrice), is(Optional.of(300L)));
    }

    private <T> Message<T> testMessage(String key, T payload) {
        return Message.message(
                key,
                of(ShardPosition.fromHorizon("")),
                payload
        );
    }
}
