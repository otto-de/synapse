package de.otto.synapse.endpoint.sender.aws;

import de.otto.synapse.endpoint.sender.AbstractMessageSenderEndpoint;
import de.otto.synapse.message.Message;
import de.otto.synapse.translator.MessageTranslator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SQSAsyncClient;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableMap.of;
import static java.lang.String.valueOf;
import static java.util.stream.Collectors.toList;

public class SqsMessageSender extends AbstractMessageSenderEndpoint {

    private static final Logger LOG = LoggerFactory.getLogger(SqsMessageSender.class);

    public static final String MSG_KEY_ATTR = "synapse_msg_key";

    private final String queueUrl;
    private final SQSAsyncClient sqsAsyncClient;

    public SqsMessageSender(final String channelName,
                            final String queueUrl,
                            final MessageTranslator<String> messageTranslator,
                            final SQSAsyncClient sqsAsyncClient) {
        super(channelName, messageTranslator);
        this.queueUrl = queueUrl;
        this.sqsAsyncClient = sqsAsyncClient;
    }

    @Override
    protected void doSend(@Nonnull Message<String> message) {
        sqsAsyncClient.sendMessage(
                SendMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .messageAttributes(Collections.singletonMap(MSG_KEY_ATTR, MessageAttributeValue.builder().dataType("String").stringValue(message.getKey()).build()))
                        .messageBody(message.getPayload())
                        .build()
        ).whenComplete((result, exception) -> {
            if (exception != null) {
                LOG.error(String.format("Failed to send message %s", message), exception);
            }
            if (result != null) {
                LOG.debug("Successfully sent message ", result);
            }
        });
    }

    @Override
    protected void doSendBatch(@Nonnull Stream<Message<String>> messageStream) {
        final AtomicInteger id = new AtomicInteger(0);
        sqsAsyncClient.sendMessageBatch(SendMessageBatchRequest.builder()
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
                .build())
                .whenComplete((result, exception) -> {
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
                });
    }

}
