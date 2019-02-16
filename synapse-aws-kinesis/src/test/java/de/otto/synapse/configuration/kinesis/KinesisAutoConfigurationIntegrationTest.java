package de.otto.synapse.configuration.kinesis;


import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpointFactory;
import de.otto.synapse.endpoint.sender.MessageSenderEndpointFactory;
import de.otto.synapse.endpoint.sender.kinesis.KinesisMessageSenderEndpointFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

@RunWith(SpringRunner.class)
@ActiveProfiles("test")
@EnableAutoConfiguration
@ComponentScan(basePackages = {"de.otto.synapse"})
@SpringBootTest(
        properties = {
                "spring.main.allow-bean-definition-overriding=true"
        },
        classes = KinesisAutoConfigurationIntegrationTest.class
)
public class KinesisAutoConfigurationIntegrationTest {

    @Autowired
    private MessageSenderEndpointFactory messageLogSenderEndpointFactory;

    @Autowired
    private MessageLogReceiverEndpointFactory messageLogReceiverEndpointFactory;

    @Test
    public void shouldInjectQualifiedMessageSenderEndpointFactories() {
        assertThat(messageLogSenderEndpointFactory, is(notNullValue()));
        assertThat(messageLogSenderEndpointFactory, instanceOf(KinesisMessageSenderEndpointFactory.class));
        assertThat(messageLogReceiverEndpointFactory, is(notNullValue()));
        assertThat(messageLogReceiverEndpointFactory, instanceOf(MessageLogReceiverEndpointFactory.class));
    }
}
