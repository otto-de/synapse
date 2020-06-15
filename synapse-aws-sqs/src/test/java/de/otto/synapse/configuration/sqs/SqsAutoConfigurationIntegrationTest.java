package de.otto.synapse.configuration.sqs;


import de.otto.synapse.endpoint.receiver.MessageQueueReceiverEndpointFactory;
import de.otto.synapse.endpoint.receiver.sqs.SqsMessageQueueReceiverEndpointFactory;
import de.otto.synapse.endpoint.sender.MessageSenderEndpointFactory;
import de.otto.synapse.endpoint.sender.sqs.SqsMessageSenderEndpointFactory;
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
@SpringBootTest(classes = SqsAutoConfigurationIntegrationTest.class)
public class SqsAutoConfigurationIntegrationTest {

    @Autowired
    private MessageSenderEndpointFactory sqsMessageQueueSenderEndpointFactory;
    @Autowired
    private MessageQueueReceiverEndpointFactory messageQueueReceiverEndpointFactory;

    @Test
    public void shouldInjectQualifiedMessageSenderEndpointFactories() {
        assertThat(sqsMessageQueueSenderEndpointFactory, is(notNullValue()));
        assertThat(sqsMessageQueueSenderEndpointFactory, instanceOf(SqsMessageSenderEndpointFactory.class));
        assertThat(messageQueueReceiverEndpointFactory, is(notNullValue()));
        assertThat(messageQueueReceiverEndpointFactory, instanceOf(SqsMessageQueueReceiverEndpointFactory.class));
    }
}
