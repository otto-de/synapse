package de.otto.synapse.configuration.aws;


import de.otto.edison.aws.configuration.AwsProperties;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpointFactory;
import de.otto.synapse.endpoint.sender.MessageSenderEndpointFactory;
import de.otto.synapse.endpoint.sender.aws.KinesisMessageSenderEndpointFactory;
import de.otto.synapse.endpoint.sender.aws.SqsMessageSenderEndpointFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SQSAsyncClient;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;
import static org.slf4j.LoggerFactory.getLogger;

@RunWith(SpringRunner.class)
@ActiveProfiles("test")
@EnableAutoConfiguration
@ComponentScan(basePackages = {"de.otto.synapse"})
@SpringBootTest(classes = AwsConfigurationIntegrationTest.class)
public class AwsConfigurationIntegrationTest {

    @Autowired
    private MessageSenderEndpointFactory messageQueueSenderEndpointFactory;

    @Autowired
    private MessageSenderEndpointFactory messageLogSenderEndpointFactory;

    @Autowired
    private MessageLogReceiverEndpointFactory messageLogReceiverEndpointFactory;


    @Test
    public void shouldInjectQualifiedMessageSenderEndpointFactories() {
        assertThat(messageQueueSenderEndpointFactory, is(notNullValue()));
        assertThat(messageQueueSenderEndpointFactory, instanceOf(SqsMessageSenderEndpointFactory.class));
        assertThat(messageLogSenderEndpointFactory, is(notNullValue()));
        assertThat(messageLogSenderEndpointFactory, instanceOf(KinesisMessageSenderEndpointFactory.class));
        assertThat(messageLogReceiverEndpointFactory, is(notNullValue()));
        assertThat(messageLogReceiverEndpointFactory, instanceOf(MessageLogReceiverEndpointFactory.class));
    }
}
