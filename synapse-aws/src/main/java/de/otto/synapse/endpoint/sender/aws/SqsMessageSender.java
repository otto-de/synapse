package de.otto.synapse.endpoint.sender.aws;

import com.google.common.collect.ImmutableMap;
import de.otto.synapse.endpoint.sender.AbstractMessageSenderEndpoint;
import de.otto.synapse.message.Message;
import de.otto.synapse.translator.MessageTranslator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import javax.annotation.Nonnull;
import java.util.concurrent.atomic.AtomicInteger;
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
    private final SqsAsyncClient SqsAsyncClient;

    public SqsMessageSender(final String channelName,
                            final String queueUrl,
                            final MessageTranslator<String> messageTranslator,
                            final SqsAsyncClient SqsAsyncClient,
                            final String messageSender) {
        super(channelName, messageTranslator);
        this.queueUrl = queueUrl;
        this.SqsAsyncClient = SqsAsyncClient;
        this.messageSender = messageSender;
    }

    @Override
    protected void doSend(@Nonnull Message<String> message) {
        SqsAsyncClient.sendMessage(
                SendMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .messageAttributes(ImmutableMap.of(
                                MSG_KEY_ATTR, MessageAttributeValue.builder().dataType("String").stringValue(message.getKey()).build(),
                                MSG_SENDER_ATTR, MessageAttributeValue.builder().dataType("String").stringValue(messageSender).build()))
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
        SqsAsyncClient.sendMessageBatch(SendMessageBatchRequest.builder()
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
