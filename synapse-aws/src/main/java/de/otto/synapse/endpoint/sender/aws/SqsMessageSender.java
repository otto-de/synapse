package de.otto.synapse.endpoint.sender.aws;

import com.google.common.collect.ImmutableMap;
import de.otto.synapse.endpoint.sender.AbstractMessageSenderEndpoint;
import de.otto.synapse.message.Message;
import de.otto.synapse.translator.MessageTranslator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.*;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableMap.of;
import static java.lang.String.valueOf;
import static java.util.stream.Collectors.toList;

public class SqsMessageSender extends AbstractMessageSenderEndpoint {

    private static final Logger LOG = LoggerFactory.getLogger(SqsMessageSender.class);

    public static final String MSG_KEY_ATTR = "synapse_msg_key";
    public static final String MSG_SENDER_ATTR = "synapse_msg_sender";

    private final String queueUrl;
    private final String messageSender;
    private final SqsAsyncClient sqsAsyncClient;

    public SqsMessageSender(final String channelName,
                            final String queueUrl,
                            final MessageTranslator<String> messageTranslator,
                            final SqsAsyncClient sqsAsyncClient,
                            final String messageSender) {
        super(channelName, messageTranslator);
        this.queueUrl = queueUrl;
        this.sqsAsyncClient = sqsAsyncClient;
        this.messageSender = messageSender;
    }

    @Override
    protected CompletableFuture<Void> doSend(final @Nonnull Message<String> message) {
        final CompletableFuture<SendMessageResponse> futureResponse = sqsAsyncClient
                .sendMessage(toSendMessageRequest(message))
                .whenComplete(logResponse(message));
        // Just because we need a CompletableFuture<Void>, no CompletableFuture<SendMessageResponse>:
        return CompletableFuture.allOf(futureResponse);
    }

    @Override
    protected CompletableFuture<Void> doSendBatch(final @Nonnull Stream<Message<String>> messageStream) {
        final CompletableFuture<SendMessageBatchResponse> futureResponse = sqsAsyncClient
                .sendMessageBatch(toSendMessageBatchRequest(messageStream))
                .whenComplete(logBatchResponse());
        // Just because we need a CompletableFuture<Void>, no CompletableFuture<SendMessageBatchResponse>:
        return CompletableFuture.allOf(futureResponse);
    }

    private SendMessageBatchRequest toSendMessageBatchRequest(final @Nonnull Stream<Message<String>> messageStream) {
        final AtomicInteger id = new AtomicInteger(0);
        return SendMessageBatchRequest.builder()
                .queueUrl(queueUrl)
                .entries(
                        messageStream.map(message -> SendMessageBatchRequestEntry.builder()
                                .id(valueOf(id.getAndIncrement()))
                                .messageAttributes(of(MSG_KEY_ATTR, MessageAttributeValue
                                        .builder()
                                        .dataType("String")
                                        .stringValue(message.getKey())
                                        .build()))
                                .messageBody(message.getPayload())
                                .build()).collect(toList())
                )
                .build();
    }

    private SendMessageRequest toSendMessageRequest(final @Nonnull Message<String> message) {
        return SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageAttributes(ImmutableMap.of(
                        MSG_KEY_ATTR, MessageAttributeValue.builder().dataType("String").stringValue(message.getKey()).build(),
                        MSG_SENDER_ATTR, MessageAttributeValue.builder().dataType("String").stringValue(messageSender).build()))
                .messageBody(message.getPayload())
                .build();
    }

    private BiConsumer<SendMessageResponse, Throwable> logResponse(final @Nonnull Message<String> message) {
        return (result, exception) -> {
            if (exception != null) {
                LOG.error(String.format("Failed to send message %s", message), exception);
            }
            if (result != null) {
                LOG.debug("Successfully sent message ", result);
            }
        };
    }

    private BiConsumer<SendMessageBatchResponse, Throwable> logBatchResponse() {
        return (result, exception) -> {
                if (exception != null) {
                    LOG.error("Failed to send batch of messages: " + exception.getMessage(), exception);
                }
                if (result != null) {
                    if (!result.successful().isEmpty()) {
                        LOG.debug("Successfully sent {} messages in a batch", result.successful().size());
                    }
                    if (!result.failed().isEmpty()) {
                        LOG.error("Failed to sent {} messages in a batch: {}", result.failed().size(), result.failed());
                    }
                }
        };
    }

}
