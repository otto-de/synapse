package de.otto.synapse.endpoint.sender.aws;

import de.otto.synapse.endpoint.sender.AbstractMessageSenderEndpoint;
import de.otto.synapse.message.Message;
import de.otto.synapse.translator.MessageTranslator;
import software.amazon.awssdk.services.sqs.SQSAsyncClient;
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
                        .messageGroupId(getChannelName())
                        .messageAttributes(of("key", MessageAttributeValue
                                .builder()
                                .stringValue(message.getKey())
                                .build()))
                        .messageBody(message.getPayload())
                        .build()
        );
    }

    @Override
    protected void doSendBatch(@Nonnull Stream<Message<String>> messageStream) {
        final AtomicInteger id = new AtomicInteger(0);
        sqsAsyncClient.sendMessageBatch(SendMessageBatchRequest.builder()
                .queueUrl(queueUrl)
                .entries(
                        messageStream.map(message -> SendMessageBatchRequestEntry.builder()
                                .id(valueOf(id.getAndIncrement()))
                                .messageGroupId(getChannelName())
                                .messageAttributes(of("key", MessageAttributeValue
                                        .builder()
                                        .stringValue(message.getKey())
                                        .build()))
                                .messageBody(message.getPayload())
                                .build()).collect(toList())
                )
                .build());
    }

}
