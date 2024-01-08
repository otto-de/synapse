package de.otto.synapse.endpoint.receiver.sqs;

import com.google.common.collect.ImmutableList;
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
import org.springframework.web.bind.annotation.*;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static de.otto.synapse.configuration.sqs.SqsTestConfiguration.SQS_INTEGRATION_TEST_CHANNEL;
import static de.otto.synapse.message.Message.message;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.joining;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

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
@DirtiesContext
public class SqsMessageQueueReceiverEndpointIntegrationTest {

    private static List<Boolean> returnError;

    @Autowired
    private MessageSenderEndpoint sqsSender;

    @Autowired
    private SqsAsyncClient asyncClient;

    private SqsAsyncClient delegateAsyncClient;

    static ReceiveMessageRequest receiveMessageRequest;

    @Before
    public void setUp() {

        SqsClientHelper sqsClientHelper = new SqsClientHelper(asyncClient);
        sqsClientHelper.createChannelIfNotExists(SQS_INTEGRATION_TEST_CHANNEL);
        sqsClientHelper.purgeQueue(SQS_INTEGRATION_TEST_CHANNEL);

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

    @After
    public void tearDown() {
        new SqsClientHelper(asyncClient).purgeQueue(SQS_INTEGRATION_TEST_CHANNEL);
    }

    @Test
    public void shouldRetryAfterTimeout() throws ExecutionException, InterruptedException {
        //given queue returns error on first request, ok on second request
        returnError = ImmutableList.of(true, false);
        final String expectedPayload = "some payload: " + LocalDateTime.now();
        sqsSender.send(message("test-key-shouldSendAndReceiveSqsMessage", expectedPayload)).join();

        //when queue returns error in first request
        receiveMessageRequest = buildReceiveMessageRequest();
        CompletableFuture<ReceiveMessageResponse> receiveMessageResponseCompletableFuture = delegateAsyncClient.receiveMessage(receiveMessageRequest);
        await()
                .atMost(5, SECONDS)
                .until(receiveMessageResponseCompletableFuture::isDone);

        //then
        String returnedMessage = receiveMessageResponseCompletableFuture.get().messages().get(0).body();
        assertThat(returnedMessage, is(expectedPayload));
        assertThat(StubController.getCount(), is(2));
    }

    private ReceiveMessageRequest buildReceiveMessageRequest() throws InterruptedException, ExecutionException {
        return ReceiveMessageRequest.builder()
                .queueUrl(
                        asyncClient.getQueueUrl(GetQueueUrlRequest
                                .builder()
                                .queueName(SQS_INTEGRATION_TEST_CHANNEL)
                                .overrideConfiguration(AwsRequestOverrideConfiguration.builder()
                                        .build())
                                .build()).get().queueUrl())
                .messageAttributeNames(".*")
                .build();
    }

    @Controller
    public static class StubController {

        private final static AtomicInteger count = new AtomicInteger();

        @Autowired
        private SqsAsyncClient asyncClient;

        @RequestMapping(value = "/**", produces = {"application/x-amz-json-1.0"})
        @ResponseBody
        public ResponseEntity<?> getResponse(@RequestBody String body, HttpServletRequest request) throws InterruptedException, ExecutionException {

            if (returnError.get(count.getAndIncrement())) {
                return new ResponseEntity<Void>(HttpStatus.BAD_GATEWAY);
            }

            ReceiveMessageResponse receiveMessageResponse = asyncClient.receiveMessage(receiveMessageRequest).get();
            return ResponseEntity.ok(mockedJsonResponse(receiveMessageResponse.messages().get(0)));
        }

        public static int getCount() {
            return count.get();
        }

        private String mockedJsonResponse(software.amazon.awssdk.services.sqs.model.Message message) {
            return "{\n" +
                    "    \"Messages\": [\n" +
                    "        {\n" +
                    "            \"Attributes\": {\n" +
                    "                \"SenderId\": \"AIDASSYFHUBOBT7F4XT75\",\n" +
                    "                \"ApproximateFirstReceiveTimestamp\": \"1677112433437\",\n" +
                    "                \"ApproximateReceiveCount\": \"1\",\n" +
                    "                \"SentTimestamp\": \"1677112427387\"\n" +
                    "            },\n" +
                    "            \"Body\": \"" + message.body() + "\",\n" +
                    "            \"MD5OfBody\": \"" + message.md5OfBody() + "\",\n" +
                    "            \"MessageId\": \"219f8380-5770-4cc2-8c3e-5c715e145f5e\",\n" +
                    "            \"ReceiptHandle\": \"AQEBaZ+j5qUoOAoxlmrCQPkBm9njMWXqemmIG6shMHCO6fV20JrQYg/AiZ8JELwLwOu5U61W+aIX5Qzu7GGofxJuvzymr4Ph53RiR0mudj4InLSgpSspYeTRDteBye5tV/txbZDdNZxsi+qqZA9xPnmMscKQqF6pGhnGIKrnkYGl45Nl6GPIZv62LrIRb6mSqOn1fn0yqrvmWuuY3w2UzQbaYunJWGxpzZze21EOBtywknU3Je/g7G9is+c6K9hGniddzhLkK1tHzZKjejOU4jokaiB4nmi0dF3JqLzDsQuPF0Gi8qffhEvw56nl8QCbluSJScFhJYvoagGnDbwOnd9z50L239qtFIgETdpKyirlWwl/NGjWJ45dqWpiW3d2Ws7q\"\n" +
                    "        }\n" +
                    "    ]\n" +
                    "}";
        }
    }
}