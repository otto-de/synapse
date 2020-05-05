package de.otto.synapse.endpoint.receiver.kafka;

import de.otto.synapse.configuration.kafka.SynapseKafkaAutoConfiguration;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpoint;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import static de.otto.synapse.endpoint.EndpointType.RECEIVER;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

@RunWith(SpringRunner.class)
@DirtiesContext
@SpringBootTest(
        classes = {
                KafkaAutoConfiguration.class,
                SynapseKafkaAutoConfiguration.class
        },
        properties = {
                "spring.kafka.bootstrap-servers=localhost:4711"
        }
)
@EmbeddedKafka(
        partitions = 1,
        ports = 4711,
        topics = KafkaMessageLogReceiverEndpointFactoryIntegrationTest.KAFKA_TOPIC)
    public class KafkaMessageLogReceiverEndpointFactoryIntegrationTest {

    public static final String KAFKA_TOPIC = "test-stream";

    @Autowired
    private KafkaMessageLogReceiverEndpointFactory endpointFactory;

    @Test
    public void shouldCreateReceiverEndpoint() throws Exception {
        final MessageLogReceiverEndpoint receiverEndpoint = endpointFactory.create("foo");
        assertThat(receiverEndpoint, is(instanceOf(KafkaMessageLogReceiverEndpoint.class)));
        assertThat(receiverEndpoint.getChannelName(), is("foo"));
        assertThat(receiverEndpoint.getEndpointType(), is(RECEIVER));
    }


}