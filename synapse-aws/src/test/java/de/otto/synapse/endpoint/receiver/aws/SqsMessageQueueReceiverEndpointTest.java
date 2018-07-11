package de.otto.synapse.endpoint.receiver.aws;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.message.Message;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.services.sqs.SQSAsyncClient;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;

public class SqsMessageQueueReceiverEndpointTest {

    private static final Pattern MATCH_ALL = Pattern.compile(".*");
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Mock
    private SQSAsyncClient sqsAsyncClient;

    @Mock
    private MessageConsumer<String> messageConsumer;

    @Captor
    private ArgumentCaptor<Message<String>> messageArgumentCaptor;

    private SqsMessageQueueReceiverEndpoint sqlQueueReceiver;
    private AtomicInteger nextKey = new AtomicInteger(0);


    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        when(messageConsumer.keyPattern()).thenReturn(MATCH_ALL);
        when(messageConsumer.payloadType()).thenReturn(String.class);
        when(sqsAsyncClient.getQueueUrl(any(GetQueueUrlRequest.class)))
                .thenReturn(completedFuture(GetQueueUrlResponse.builder().queueUrl("http://example.org/test").build()));
    }

    @Test
    public void shouldConsumeMessages() throws InterruptedException {
        //given
        final CountDownLatch latch = new CountDownLatch(3);
        final List<Message<String>> messages = new ArrayList<>();
        final MessageConsumer<String> messageConsumer = MessageConsumer.of(".*", String.class, (m)->{
            latch.countDown();
            messages.add(m);
        });
        sqlQueueReceiver = new SqsMessageQueueReceiverEndpoint("channelName", sqsAsyncClient, objectMapper, null);
        sqlQueueReceiver.register(messageConsumer);

        String payload1 = "{\"data\":\"red\"}";
        String payload2 = "{\"data\":\"green\"}";
        String payload3 = "{\"data\":\"blue\"}";

        addSqsMessagesToQueue(ImmutableList.of(
                sqsMessage(payload1),
                sqsMessage(payload2),
                sqsMessage(payload3))
        );

        // when consumtion is started
        sqlQueueReceiver.consume();

        latch.await(2, SECONDS);

        // then
        assertThat(messages, hasSize(3));
        assertThat(messages.get(0).getPayload(), is(payload1));
        assertThat(messages.get(1).getPayload(), is(payload2));
        assertThat(messages.get(2).getPayload(), is(payload3));
    }

    private software.amazon.awssdk.services.sqs.model.Message sqsMessage(String body) {
        return software.amazon.awssdk.services.sqs.model.Message
                .builder()
                .body(body)
                .build();
    }

    private void addSqsMessagesToQueue(Collection<software.amazon.awssdk.services.sqs.model.Message> sqsMessages) {
        //and: some records
        ReceiveMessageResponse response1 = ReceiveMessageResponse.builder()
                .messages(sqsMessages)
                .build();

        when(sqsAsyncClient.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenReturn(completedFuture(response1));
    }

    @Test
    public void shouldDeleteMessageAfterConsume() {
        fail();
    }

    @Test
    public void shouldInterceptMessages() {
        fail();
    }

    @Test
    public void shouldNotConsumeMessagesDroppedByInterceptor() {
        fail();
    }

    @Test
    public void shouldIgnoreSdkClientExceptions() {
        fail();
    }

    @Test
    public void shouldIgnoreSQSExceptions() {
        fail();
    }

}