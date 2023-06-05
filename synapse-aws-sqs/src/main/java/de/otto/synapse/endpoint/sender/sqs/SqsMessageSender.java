package de.otto.synapse.endpoint.sender.sqs;

import com.google.common.collect.ImmutableMap;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.sender.AbstractMessageSenderEndpoint;
import de.otto.synapse.message.TextMessage;
import de.otto.synapse.translator.MessageTranslator;
import jakarta.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import static java.lang.String.valueOf;
import static java.util.stream.Collectors.toList;

public class SqsMessageSender extends AbstractMessageSenderEndpoint {

    private static final Logger LOG = LoggerFactory.getLogger(SqsMessageSender.class);

    public static final String MSG_KEY_ATTR = "synapse_msg_key";

    private final String queueUrl;
    private final SqsAsyncClient sqsAsyncClient;

    public SqsMessageSender(final String channelName,
                            final String queueUrl,
                            final MessageInterceptorRegistry interceptorRegistry,
                            final MessageTranslator<TextMessage> messageTranslator,
                            final SqsAsyncClient sqsAsyncClient) {
        super(channelName, interceptorRegistry, messageTranslator);
        this.queueUrl = queueUrl;
        this.sqsAsyncClient = sqsAsyncClient;
    }

    @Override
    protected CompletableFuture<Void> doSend(final @Nonnull TextMessage message) {
        final CompletableFuture<SendMessageResponse> futureResponse = sqsAsyncClient
                .sendMessage(toSendMessageRequest(message))
                .whenComplete(logResponse(message));
        // Just because we need a CompletableFuture<Void>, no CompletableFuture<SendMessageResponse>:
        return CompletableFuture.allOf(futureResponse);
    }

    @Override
    protected CompletableFuture<Void> doSendBatch(final @Nonnull Stream<TextMessage> messageStream) {
        final SendMessageBatchRequest sendMessageBatchRequest = toSendMessageBatchRequest(messageStream);
        if (!sendMessageBatchRequest.entries().isEmpty()) {
            final CompletableFuture<SendMessageBatchResponse> futureResponse = sqsAsyncClient
                    .sendMessageBatch(sendMessageBatchRequest)
                    .whenComplete(logBatchResponse());
            // TODO: Introduce a response object and return it instead of Void
            // Just because we need a CompletableFuture<Void>, no CompletableFuture<SendMessageBatchResponse>:
            return CompletableFuture.allOf(futureResponse);
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    private SendMessageBatchRequest toSendMessageBatchRequest(final @Nonnull Stream<TextMessage> messageStream) {
        final AtomicInteger id = new AtomicInteger(0);
        return SendMessageBatchRequest.builder()
                .queueUrl(queueUrl)
                .entries(
                        messageStream.map(message -> SendMessageBatchRequestEntry.builder()
                                .id(valueOf(id.getAndIncrement()))
                                .messageAttributes(of(message))
                                .messageBody(message.getPayload())
                                .build()).collect(toList())
                )
                .build();
    }

    private SendMessageRequest toSendMessageRequest(final @Nonnull TextMessage message) {
        return SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageAttributes(of(message))
                .messageBody(message.getPayload())
                .build();
    }

    private ImmutableMap<String, MessageAttributeValue> of(@Nonnull TextMessage message) {
        final ImmutableMap.Builder<String, MessageAttributeValue> messageAttributes = ImmutableMap.builder();
        message.getHeader().getAll().entrySet().forEach(entry -> {
            messageAttributes.put(entry.getKey(), MessageAttributeValue
                    .builder()
                    .dataType("String")
                    .stringValue(entry.getValue())
                    .build());
        });
        messageAttributes.put(MSG_KEY_ATTR, MessageAttributeValue
                .builder()
                .dataType("String")
                .stringValue(message.getKey().partitionKey())
                .build());
        return messageAttributes.build();
    }

    private BiConsumer<SendMessageResponse, Throwable> logResponse(final @Nonnull TextMessage message) {
        return (result, exception) -> {
            if (exception != null) {
                LOG.error(String.format("Failed to send message %s", message), exception);
            }
            if (result != null) {
                LOG.debug("Successfully sent message {}", result);
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
