package de.otto.synapse.endpoint.receiver.sqs;

import com.google.common.collect.ImmutableList;
import de.otto.synapse.annotation.EnableMessageSenderEndpoint;
import de.otto.synapse.channel.selector.MessageQueue;
import de.otto.synapse.configuration.aws.AwsProperties;
import de.otto.synapse.configuration.sqs.SqsAutoConfiguration;
import de.otto.synapse.endpoint.SqsClientHelper;
import de.otto.synapse.endpoint.sender.MessageSenderEndpoint;
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
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import javax.servlet.http.HttpServletRequest;
import java.net.URI;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
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
        awsProperties.setRegion(Region.EU_CENTRAL_1.id());

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

        @RequestMapping(value = "/**", produces = {"text/xml"})
        @ResponseBody
        public ResponseEntity<?> getResponse(@RequestBody String body, HttpServletRequest request) throws InterruptedException, ExecutionException {

            System.err.println("Counter: " + count.get());
            System.err.println(body);

            System.err.println(request);

            if (returnError.get(count.getAndIncrement())) {
                return new ResponseEntity<Void>(HttpStatus.BAD_GATEWAY);
            }

            ReceiveMessageResponse receiveMessageResponse = asyncClient.receiveMessage(receiveMessageRequest).get();
            return ResponseEntity.ok(mockedXmlResponse(receiveMessageResponse.messages().get(0)));
        }

        public static int getCount() {
            return count.get();
        }

        private String mockedXmlResponse(software.amazon.awssdk.services.sqs.model.Message message) {
            return "<ReceiveMessageResponse xmlns=\"http://queue.amazonaws.com/doc/2012-11-05/\">\n" +
                    "              <ReceiveMessageResult>\n" +
                    "                <Message>\n" +
                    "                  <MessageId>" + message.messageId() + "</MessageId>\n" +
                    "                  <ReceiptHandle>" + message.receiptHandle() + "</ReceiptHandle>\n" +
                    "                  <MD5OfBody>" + message.md5OfBody() + "</MD5OfBody>\n" +
                    "                  <Body>" + message.body() + "</Body>\n" +
                    "                  \n" +
                    "                  <MD5OfMessageAttributes>" + message.md5OfMessageAttributes() + "</MD5OfMessageAttributes>\n" +
                    "                 " + mockedXmlMessageAttributes(message.messageAttributes()) +
                    "                </Message>\n" +
                    "              </ReceiveMessageResult>\n" +
                    "              <ResponseMetadata>\n" +
                    "                <RequestId>00000000-0000-0000-0000-000000000000</RequestId>\n" +
                    "              </ResponseMetadata>\n" +
                    "            </ReceiveMessageResponse>";
        }

        private String mockedXmlMessageAttributes(Map<String, MessageAttributeValue> messageAttributes) {
            return messageAttributes.keySet().stream().map(key ->
                    "<MessageAttribute>\n" +
                            "                         <Name>" + key + "</Name>\n" +
                            "                         <Value>\n" +
                            "                           <DataType>" + messageAttributes.get(key).dataType() + "</DataType>\n" +
                            "                           <StringValue>" + messageAttributes.get(key).stringValue() + "</StringValue>\n" +
                            "                         </Value>\n" +
                            "                       </MessageAttribute>\\n"
            ).collect(joining());
        }
    }
}