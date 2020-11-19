package de.otto.synapse.endpoint.sender.sqs;

import com.fasterxml.jackson.annotation.JsonProperty;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.message.Header;
import de.otto.synapse.message.Message;
import de.otto.synapse.message.TextMessage;
import de.otto.synapse.translator.MessageTranslator;
import de.otto.synapse.translator.TextMessageTranslator;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.stream.Stream;

import static com.google.common.collect.ImmutableMap.of;
import static de.otto.synapse.endpoint.MessageInterceptorRegistration.senderChannelsWith;
import static de.otto.synapse.message.Message.message;
import static java.util.Arrays.asList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class SqsMessageSenderTest {

    private SqsMessageSender sqsMessageSender;

    @Mock
    private SqsAsyncClient sqsAsyncClient;
    @Captor
    private ArgumentCaptor<SendMessageRequest> requestArgumentCaptor;
    @Captor
    private ArgumentCaptor<SendMessageBatchRequest> batchRequestArgumentCaptor;
    private MessageTranslator<TextMessage> messageTranslator = new TextMessageTranslator();
    private MessageInterceptorRegistry interceptorRegistry;

    @Before
    public void setUp() {
        interceptorRegistry = new MessageInterceptorRegistry();
        sqsMessageSender = new SqsMessageSender("test", "https://example.com/test", interceptorRegistry, messageTranslator, sqsAsyncClient);
    }

    @Test
    public void shouldSendEvent() {
        // given
        final Message<ExampleJsonObject> message = message("some-of", new ExampleJsonObject("banana"));

        when(sqsAsyncClient.sendMessage(any(SendMessageRequest.class))).thenReturn(completedFuture(SendMessageResponse.builder()
                .sequenceNumber("42")
                .messageId("some-id")
                .build()));

        // when
        sqsMessageSender.send(message).join();

        // then
        verify(sqsAsyncClient).sendMessage(requestArgumentCaptor.capture());
        final SendMessageRequest capturedRequest = requestArgumentCaptor.getValue();

        assertThat(capturedRequest.queueUrl(), is("https://example.com/test"));
        assertThat(capturedRequest.messageBody(), is("{\"value\":\"banana\"}"));
    }

    @Test
    public void shouldSendKeyAsMessageHeader() {
        // given
        final Message<ExampleJsonObject> message = message("some-of", new ExampleJsonObject("banana"));

        when(sqsAsyncClient.sendMessage(any(SendMessageRequest.class))).thenReturn(completedFuture(SendMessageResponse.builder()
                .sequenceNumber("42")
                .messageId("some-id")
                .build()));

        // when
        sqsMessageSender.send(message).join();

        // then
        verify(sqsAsyncClient).sendMessage(requestArgumentCaptor.capture());
        final SendMessageRequest capturedRequest = requestArgumentCaptor.getValue();

        assertThat(capturedRequest.messageAttributes(), hasEntry("synapse_msg_key", MessageAttributeValue.builder().dataType("String").stringValue("some-of").build()));
    }

    @Test
    public void shouldSendCustomMessageHeader() {
        // given
        final Message<ExampleJsonObject> message = message(
                "some-of",
                Header.of(of("first", "one", "second", "two")),
                new ExampleJsonObject("banana"));

        when(sqsAsyncClient.sendMessage(any(SendMessageRequest.class))).thenReturn(completedFuture(SendMessageResponse.builder()
                .sequenceNumber("42")
                .messageId("some-id")
                .build()));

        // when
        sqsMessageSender.send(message).join();

        // then
        verify(sqsAsyncClient).sendMessage(requestArgumentCaptor.capture());
        final SendMessageRequest capturedRequest = requestArgumentCaptor.getValue();

        assertThat(capturedRequest.messageAttributes(), hasEntry("first", MessageAttributeValue.builder().dataType("String").stringValue("one").build()));
        assertThat(capturedRequest.messageAttributes(), hasEntry("second", MessageAttributeValue.builder().dataType("String").stringValue("two").build()));
    }

    @Test
    public void shouldInterceptMessages() {
        // given
        final Message<ExampleJsonObject> message = message("", new ExampleJsonObject("banana"));

        when(sqsAsyncClient.sendMessage(any(SendMessageRequest.class))).thenReturn(completedFuture(SendMessageResponse.builder()
                .sequenceNumber("42")
                .messageId("some-id")
                .build()));
        // and especially
        interceptorRegistry.register(senderChannelsWith((m) -> TextMessage.of(m.getKey(), m.getHeader(), "{\"value\":\"apple\"}")));

        // when
        sqsMessageSender.send(message).join();

        // then
        verify(sqsAsyncClient).sendMessage(requestArgumentCaptor.capture());
        final SendMessageRequest capturedRequest = requestArgumentCaptor.getValue();

        assertThat(capturedRequest.messageBody(), is("{\"value\":\"apple\"}"));
    }

    @Test
    public void shouldNotSendMessagesDroppedByInterceptor() {
        // given
        final Message<ExampleJsonObject> message = message("", new ExampleJsonObject("banana"));

        interceptorRegistry.register(senderChannelsWith((m) -> null));

        // when
        sqsMessageSender.send(message).join();

        // then
        verifyNoMoreInteractions(sqsAsyncClient);
    }

    @Test
    public void shouldSendBatch() {
        // given
        ExampleJsonObject bananaObject = new ExampleJsonObject("banana");
        ExampleJsonObject appleObject = new ExampleJsonObject("apple");

        when(sqsAsyncClient.sendMessageBatch(any(SendMessageBatchRequest.class))).thenReturn(completedFuture(SendMessageBatchResponse.builder()
                .successful(asList(
                        SendMessageBatchResultEntry.builder().build(),
                        SendMessageBatchResultEntry.builder().build()
                ))
                .build()));

        // when
        sqsMessageSender.sendBatch(Stream.of(
                message("b", bananaObject),
                message("a", appleObject)
        ));

        // then
        verify(sqsAsyncClient).sendMessageBatch(batchRequestArgumentCaptor.capture());
        final SendMessageBatchRequest capturedRequest = batchRequestArgumentCaptor.getValue();

        assertThat(capturedRequest.entries(), hasSize(2));
        assertThat(capturedRequest.entries().get(0).messageAttributes(), hasEntry("synapse_msg_key", MessageAttributeValue.builder().dataType("String").stringValue("b").build()));
        assertThat(capturedRequest.entries().get(0).messageBody(), is("{\"value\":\"banana\"}"));
        assertThat(capturedRequest.entries().get(1).messageAttributes(), hasEntry("synapse_msg_key", MessageAttributeValue.builder().dataType("String").stringValue("a").build()));
        assertThat(capturedRequest.entries().get(1).messageBody(), is("{\"value\":\"apple\"}"));
    }

    @Test
    public void shouldSendBatchAndInterceptMessages() {
        // given
        ExampleJsonObject bananaObject = new ExampleJsonObject("banana");
        ExampleJsonObject appleObject = new ExampleJsonObject("apple");

        when(sqsAsyncClient.sendMessageBatch(any(SendMessageBatchRequest.class))).thenReturn(completedFuture(SendMessageBatchResponse.builder()
                .successful(asList(
                        SendMessageBatchResultEntry.builder().build(),
                        SendMessageBatchResultEntry.builder().build()
                ))
                .build()));

        // and especially
        interceptorRegistry.register(
                senderChannelsWith((m) -> TextMessage.of(m.getKey(), m.getHeader(), "{\"value\" : \"Lovely day for a Guinness\"}"))
        );

        // when
        sqsMessageSender.sendBatch(Stream.of(
                message("b", bananaObject),
                message("a", appleObject)
        ));

        // then
        verify(sqsAsyncClient).sendMessageBatch(batchRequestArgumentCaptor.capture());
        final SendMessageBatchRequest capturedRequest = batchRequestArgumentCaptor.getValue();

        assertThat(capturedRequest.entries(), hasSize(2));
        assertThat(capturedRequest.entries().get(0).messageBody(), is("{\"value\" : \"Lovely day for a Guinness\"}"));
        assertThat(capturedRequest.entries().get(1).messageBody(), is("{\"value\" : \"Lovely day for a Guinness\"}"));
    }

    @Test
    public void shouldNotSendEmptyBatch() {
        // given
        ExampleJsonObject bananaObject = new ExampleJsonObject("banana");
        ExampleJsonObject appleObject = new ExampleJsonObject("apple");

        // and especially drop all messages
        interceptorRegistry.register(
                senderChannelsWith((m) -> null)
        );

        // when
        sqsMessageSender.sendBatch(Stream.of(
                message("b", bananaObject),
                message("a", appleObject)
        ));

        // then
        verifyNoMoreInteractions(sqsAsyncClient);
    }

    @Test
    public void shouldSendDeleteEventWithEmptyByteBuffer() {
        // given
        when(sqsAsyncClient.sendMessage(any(SendMessageRequest.class))).thenReturn(completedFuture(SendMessageResponse.builder()
                .sequenceNumber("42")
                .messageId("some-id")
                .build()));

        //when
        sqsMessageSender.send(message("", null)).join();

        //then
        verify(sqsAsyncClient).sendMessage(requestArgumentCaptor.capture());
        assertThat(requestArgumentCaptor.getValue().messageBody(), is(nullValue()));
    }

    private static class ExampleJsonObject {
        @JsonProperty
        private String value;

        public ExampleJsonObject() {
        }

        ExampleJsonObject(String value) {
            this.value = value;
        }

    }
}
