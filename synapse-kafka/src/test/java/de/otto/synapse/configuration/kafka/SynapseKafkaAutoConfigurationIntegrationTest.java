package de.otto.synapse.configuration.kafka;


import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpointFactory;
import de.otto.synapse.endpoint.receiver.kafka.KafkaMessageLogReceiverEndpointFactory;
import de.otto.synapse.endpoint.sender.MessageSenderEndpointFactory;
import de.otto.synapse.endpoint.sender.kafka.KafkaMessageSenderEndpointFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

@RunWith(SpringRunner.class)
@ActiveProfiles("test")
@EnableAutoConfiguration
@ComponentScan(basePackages = {"de.otto.synapse"})
@DirtiesContext
@EmbeddedKafka(
        partitions = 1,
        topics = SynapseKafkaAutoConfigurationIntegrationTest.KAFKA_TOPIC)
@SpringBootTest(
        properties = {
                "spring.main.allow-bean-definition-overriding=true"
        },
        classes = SynapseKafkaAutoConfigurationIntegrationTest.class
)
public class SynapseKafkaAutoConfigurationIntegrationTest {

    public static final String KAFKA_TOPIC = "test-stream";

    @Autowired
    private MessageSenderEndpointFactory messageSenderEndpointFactory;

    @Autowired
    private MessageLogReceiverEndpointFactory messageLogReceiverEndpointFactory;

    @Test
    public void shouldInjectQualifiedMessageSenderEndpointFactories() {
        assertThat(messageSenderEndpointFactory, is(notNullValue()));
        assertThat(messageSenderEndpointFactory, instanceOf(KafkaMessageSenderEndpointFactory.class));
        assertThat(messageLogReceiverEndpointFactory, is(notNullValue()));
        assertThat(messageLogReceiverEndpointFactory, instanceOf(KafkaMessageLogReceiverEndpointFactory.class));
    }
}
