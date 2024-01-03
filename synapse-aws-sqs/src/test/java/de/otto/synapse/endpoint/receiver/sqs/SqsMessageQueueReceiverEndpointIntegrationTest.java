package de.otto.synapse.endpoint.receiver.sqs;

import de.otto.synapse.annotation.EnableMessageSenderEndpoint;
import de.otto.synapse.channel.selector.MessageQueue;
import de.otto.synapse.configuration.aws.AwsProperties;
import de.otto.synapse.configuration.sqs.SqsAutoConfiguration;
import de.otto.synapse.endpoint.SqsClientHelper;
import de.otto.synapse.endpoint.sender.MessageSenderEndpoint;
import jakarta.servlet.http.HttpServletRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutionException;

import static de.otto.synapse.configuration.sqs.SqsTestConfiguration.SQS_INTEGRATION_TEST_CHANNEL;
import static de.otto.synapse.message.Message.message;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

@RunWith(SpringRunner.class)
@ActiveProfiles("test")
@EnableAutoConfiguration
@ComponentScan(basePackages = {"de.otto.synapse"})
@SpringBootTest(
        properties = {
                "spring.main.allow-bean-definition-overriding=true"
        },
        webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT,
        classes = SqsMessageQueueReceiverEndpointIntegrationTest.class
)
@EnableMessageSenderEndpoint(
        name = "sqsSender",
        channelName = SQS_INTEGRATION_TEST_CHANNEL,
        selector = MessageQueue.class)
public class SqsMessageQueueReceiverEndpointIntegrationTest {

    @Autowired
    private MessageSenderEndpoint sqsSender;

    private SqsAsyncClient delegateAsyncClient;

    static ReceiveMessageRequest receiveMessageRequest;

    static int stubControllerCalls;

    @Before
    public void setUp() {
        AwsProperties awsProperties = new AwsProperties();
        awsProperties.setRegion(Region.US_EAST_1.id());

        delegateAsyncClient = SqsAsyncClient.builder()
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("foobar", "foobar")))
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .apiCallAttemptTimeout(Duration.ofMillis(500))
                        .retryPolicy(new SqsAutoConfiguration(awsProperties)
                                .sqsRetryPolicy()).build())
                .endpointOverride(URI.create("http://localhost:8080/"))
                .build();
    }

    @Test
    public void shouldRetryOnFailure() throws ExecutionException, InterruptedException {
        //given
        final String expectedPayload = "some payload: " + LocalDateTime.now();
        sqsSender.send(message("test-key-shouldSendAndReceiveSqsMessage", expectedPayload)).join();

        //when queue returns error
        receiveMessageRequest = buildReceiveMessageRequest();
        delegateAsyncClient.receiveMessage(receiveMessageRequest);

        //then
        await()
                .atMost(5, SECONDS)
                .until(() -> stubControllerCalls == 2);
    }

    private ReceiveMessageRequest buildReceiveMessageRequest() throws InterruptedException, ExecutionException {
        return ReceiveMessageRequest.builder()
                .queueUrl("http://sqs.cloud/000000000000/sqs-test-channel")
                .messageAttributeNames(".*")
                .build();
    }

    @Controller
    public static class StubController {

        @RequestMapping(value = "/**", produces = {"text/xml"})
        @ResponseBody
        public ResponseEntity<?> getResponse(@RequestBody String body, HttpServletRequest request) {
            stubControllerCalls++;
            return new ResponseEntity<Void>(HttpStatus.BAD_GATEWAY);

        }
    }
}