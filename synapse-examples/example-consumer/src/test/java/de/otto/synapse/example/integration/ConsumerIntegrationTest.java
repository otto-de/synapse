package de.otto.synapse.example.integration;

import com.jayway.awaitility.Awaitility;
import de.otto.synapse.endpoint.sender.MessageSenderEndpoint;
import de.otto.synapse.endpoint.sender.MessageSenderEndpointFactory;
import de.otto.synapse.example.consumer.Server;
import de.otto.synapse.example.consumer.configuration.MyServiceProperties;
import de.otto.synapse.example.consumer.payload.BananaPayload;
import de.otto.synapse.example.consumer.state.BananaProduct;
import de.otto.synapse.state.StateRepository;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static de.otto.synapse.message.Message.message;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {Server.class})
@ActiveProfiles("test")
public class ConsumerIntegrationTest {

    @Autowired
    StateRepository<BananaProduct> bananaProductStateRepository;

    @Autowired
    MessageSenderEndpointFactory messageSenderFactory;

    @Autowired
    MyServiceProperties properties;

    private MessageSenderEndpoint bananaSender;

    @Before
    public void setUp() throws Exception {
        bananaSender = messageSenderFactory.create(properties.getBananaChannel());
    }

    @Test
    public void shouldRetrieveBananasFromStream() {
        // given
        BananaPayload bananaPayload = new BananaPayload();
        bananaPayload.setId("banana_id");
        bananaPayload.setColor("yellow");

        // when
        bananaSender.send(message("banana_id", bananaPayload));

        // then
        Awaitility.await()
                .atMost(3, TimeUnit.SECONDS)
                .until(() -> bananaProductStateRepository.get("banana_id").isPresent());

        Optional<BananaProduct> optionalBananaProduct = bananaProductStateRepository.get("banana_id");
        assertThat(optionalBananaProduct.map(BananaProduct::getColor), is(Optional.of("yellow")));
    }
}
