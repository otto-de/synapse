package de.otto.synapse.endpoint.receiver.aws;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.endpoint.MessageInterceptor;
import de.otto.synapse.message.Message;
import org.awaitility.Duration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.sqs.SQSAsyncClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static java.util.Collections.synchronizedList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class SqsMessageQueueReceiverEndpointTest {

    private static final int EXPECTED_NUMBER_OF_ENTRIES = 3;

    private static final String PAYLOAD_1 = "{\"data\":\"red\"}";
    private static final String PAYLOAD_2 = "{\"data\":\"green\"}";
    private static final String PAYLOAD_3 = "{\"data\":\"blue\"}";
    private static final String INTERCEPTED_PAYLOAD = "{\"data\":\"intercepted\"}";
    private static final String QUEUE_URL = "http://example.org/test";


    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Mock
    private SQSAsyncClient sqsAsyncClient;

    private SqsMessageQueueReceiverEndpoint sqsQueueReceiver;
    private List<Message<String>> messages = synchronizedList(new ArrayList<>());


    @Before
    public void setUp() {
        messages.clear();
        MockitoAnnotations.initMocks(this);
        when(sqsAsyncClient.getQueueUrl(any(GetQueueUrlRequest.class)))
                .thenReturn(completedFuture(GetQueueUrlResponse.builder().queueUrl(QUEUE_URL).build()));

        sqsQueueReceiver = new SqsMessageQueueReceiverEndpoint("channelName", sqsAsyncClient, objectMapper, null);
        sqsQueueReceiver.register(MessageConsumer.of(".*", String.class, (message) -> messages.add(message)));

    }

    @After
    public void after() {
        sqsQueueReceiver.stop();
    }

    /**
     * Currently, the SQS implementation is not able to use message keys, so it is not possible to use
     * MessageConsumers with key filters other than .*
     * This should be changed, as soon as localstack is able to handle message attributes without returning HTTP 500.
     */
    @Test(expected = IllegalStateException.class)
    public void shouldFailToFilterMessagesByKey() {
        sqsQueueReceiver = new SqsMessageQueueReceiverEndpoint("channelName", sqsAsyncClient, objectMapper, null);
        sqsQueueReceiver.register(MessageConsumer.of("somekeyFilter", String.class, (message) -> messages.add(message)));
        sqsQueueReceiver.consume();
    }

    @Test(expected = RuntimeException.class)
    public void shouldShutdownOnRuntimeExceptionOnGetQueueUrl() {

        //given:
        when(sqsAsyncClient.getQueueUrl(any(GetQueueUrlRequest.class)))
                .thenThrow(RuntimeException.class);


        sqsQueueReceiver = new SqsMessageQueueReceiverEndpoint("channelName", sqsAsyncClient, objectMapper, null);
    }

    @Test
    public void shouldConsumeMessages() {
        // given:
        addSqsMessagesToQueue(
                sqsMessage(PAYLOAD_1),
                sqsMessage(PAYLOAD_2),
                sqsMessage(PAYLOAD_3));

        // when: consumption is started
        sqsQueueReceiver.consume();

        // then:
        // wait some time
        await()
                .atMost(Duration.FIVE_SECONDS)
                .until(() -> messages.size() >= EXPECTED_NUMBER_OF_ENTRIES);

        // and:
        // expect the payload to be the added messages
        assertThat(messages.size(), is(3));
        assertThat(messages.get(0).getPayload(), is(PAYLOAD_1));
        assertThat(messages.get(1).getPayload(), is(PAYLOAD_2));
        assertThat(messages.get(2).getPayload(), is(PAYLOAD_3));
    }

    @Test
    public void shouldDeleteMessageAfterConsume() {
        //given
        addSqsMessagesToQueue(
                sqsMessage(PAYLOAD_1),
                sqsMessage(PAYLOAD_2),
                sqsMessage(PAYLOAD_3));

        ArgumentCaptor<DeleteMessageRequest> deleteRequestCaptor = ArgumentCaptor.forClass(DeleteMessageRequest.class);

        // when: consumption is started
        sqsQueueReceiver.consume();

        // then:
        // wait until the messages are consumed
        await()
                .atMost(Duration.FIVE_SECONDS)
                .until(() -> messages.size() >= EXPECTED_NUMBER_OF_ENTRIES);

        // and:
        // expect delete message to be executed
        verify(sqsAsyncClient, times(3)).deleteMessage(deleteRequestCaptor.capture());

        //and: the request should contain the queue url
        List<DeleteMessageRequest> deleteMessageRequests = deleteRequestCaptor.getAllValues();
        deleteMessageRequests.forEach(req -> {
            assertThat(req.queueUrl(), is(QUEUE_URL));
        });
    }

    @Test
    public void shouldInterceptMessages() {
        // given:
        addSqsMessagesToQueue(sqsMessage(PAYLOAD_1));

        sqsQueueReceiver.getInterceptorChain().register((message -> Message.message(message.getKey(), message.getHeader(), INTERCEPTED_PAYLOAD)));

        // when: consumption is started
        sqsQueueReceiver.consume();

        // then:
        // wait some time
        await()
                .atMost(Duration.FIVE_SECONDS)
                .until(() -> messages.size() == 1);

        // and:
        // expect the payload to be the added messages
        assertThat(messages.size(), is(1));
        assertThat(messages.get(0).getPayload(), is(INTERCEPTED_PAYLOAD));
    }

    @Test
    public void shouldNotConsumeMessagesDroppedByInterceptor() {
        // given:
        addSqsMessagesToQueue(sqsMessage(PAYLOAD_1), sqsMessage(PAYLOAD_2));

        sqsQueueReceiver.getInterceptorChain().register((message ->
                message.getPayload().equals(PAYLOAD_1) ? null : message));

        // when: consumption is started
        sqsQueueReceiver.consume();

        // then:
        // wait some time
        await()
                .atMost(Duration.FIVE_SECONDS)
                .until(() -> messages.size() == 1);

        // and:
        // expect the payload to be the added messages
        assertThat(messages.size(), is(1));
        assertThat(messages.get(0).getPayload(), is(PAYLOAD_2));
    }

    @Test(expected = RuntimeException.class)
    public void shouldShutdownServiceOnRuntimeExceptionOnConsume() throws Throwable {
        //given
        when(sqsAsyncClient.receiveMessage(any(ReceiveMessageRequest.class))).thenThrow(RuntimeException.class); // could be SdkException, SQSException etc.

        //then
        expectExceptionToBeThrownAndNotDeleteMessage();

    }

    @Test(expected = RuntimeException.class)
    public void shouldShutdownServiceOnRuntimeExceptionOnDelete() throws Throwable {
        //given
        addSqsMessagesToQueue(sqsMessage(PAYLOAD_1));
        when(sqsAsyncClient.deleteMessage(any(DeleteMessageRequest.class))).thenThrow(RuntimeException.class); // could be SdkException, SQSException etc.

        //then
        expectExceptionToBeThrownAndWithDeleteMessage();

    }

    private void expectExceptionToBeThrownAndNotDeleteMessage() throws Throwable{
        ArgumentCaptor<DeleteMessageRequest> deleteRequestCaptor = ArgumentCaptor.forClass(DeleteMessageRequest.class);

        try {
            sqsQueueReceiver.consume().get();
            fail();
        } catch (ExecutionException e) {
            // and:
            // expect delete message to be executed
            verify(sqsAsyncClient, never()).deleteMessage(deleteRequestCaptor.capture());
            throw e.getCause();
        }
    }

    private void expectExceptionToBeThrownAndWithDeleteMessage() throws Throwable{
        ArgumentCaptor<DeleteMessageRequest> deleteRequestCaptor = ArgumentCaptor.forClass(DeleteMessageRequest.class);

        try {
            sqsQueueReceiver.consume().get();
            fail();
        } catch (ExecutionException e) {
            // and:
            // expect delete message to be executed
            verify(sqsAsyncClient, times(1)).deleteMessage(deleteRequestCaptor.capture());
            throw e.getCause();
        }
    }

    private void addSqsMessagesToQueue(software.amazon.awssdk.services.sqs.model.Message... sqsMessages) {
        //and: some records
        ReceiveMessageResponse response1 = ReceiveMessageResponse.builder()
                .messages(sqsMessages)
                .build();
        ReceiveMessageResponse emptyResponse = ReceiveMessageResponse.builder()
                .messages(ImmutableList.of())
                .build();

        when(sqsAsyncClient.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenReturn(completedFuture(response1))
                //add empty response to not add messages within loop
                .thenReturn(CompletableFuture.completedFuture(emptyResponse));
    }

    private software.amazon.awssdk.services.sqs.model.Message sqsMessage(String body) {
        return software.amazon.awssdk.services.sqs.model.Message
                .builder()
                .body(body)
                .build();
    }
}