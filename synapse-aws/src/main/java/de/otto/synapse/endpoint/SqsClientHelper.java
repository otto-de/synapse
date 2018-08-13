package de.otto.synapse.endpoint;

import software.amazon.awssdk.services.sqs.SQSAsyncClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static de.otto.synapse.endpoint.sender.aws.SqsMessageSender.MSG_KEY_ATTR;
import static java.time.Instant.now;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonMap;

/**
 * A helper used to write tests for SQS senders or publishers. Not really recommended for production code.
 */
public class SqsClientHelper {

    private final SQSAsyncClient sqsAsyncClient;

    public SqsClientHelper(final SQSAsyncClient sqsAsyncClient) {
        this.sqsAsyncClient = sqsAsyncClient;
    }

    public boolean doesChannelExist(final String channelName) {
        try {
            final String queueUrl = getQueueUrl(channelName);
            return getQueues().contains(queueUrl);
        } catch (final RuntimeException e) {
            return false;
        }
    }

    public List<String> getQueues() {
        try {
            final ListQueuesResponse queuesResponse = sqsAsyncClient.listQueues().get();
            return queuesResponse.queueUrls();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public String getQueueUrl(final String channelName) {
        try {
            return sqsAsyncClient
                    .getQueueUrl(GetQueueUrlRequest.builder().queueName(channelName).build())
                    .get()
                    .queueUrl();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public void createChannelIfNotExists(final String channelName) {
        try {
            if (!doesChannelExist(channelName)) {
                sqsAsyncClient
                        .createQueue(CreateQueueRequest.builder()
                                .queueName(channelName)
                                .build()).get();
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public void purgeQueue(final String queueName) {
        try {
            if (doesChannelExist(queueName)) {
                sqsAsyncClient.purgeQueue(PurgeQueueRequest
                        .builder()
                        .queueUrl(queueName)
                        .build())
                        .get();
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public List<Message> receiveMessages(final String queueName,
                                         final int seconds) {
        final Instant started = now();
        List<Message> messages;
        boolean abort;
        do {
            messages = receiveMessages(queueName);
            abort = Duration.between(started, now()).getSeconds() > seconds;
        } while (messages.isEmpty() && !abort);
        return messages;
    }

    public void sendMessage(final String channelName, final String key, final String payload) {
        try {
            final SendMessageResponse response = sqsAsyncClient.sendMessage(SendMessageRequest.builder()
                    .queueUrl(getQueueUrl(channelName))
                    .messageAttributes(
                            singletonMap(MSG_KEY_ATTR, MessageAttributeValue.builder().dataType("String").stringValue(key).build()))
                    .messageBody(payload)
                    .build()
            ).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public List<Message> receiveMessages(final String queueName) {
        try {
            final String queueUrl = getQueueUrl(queueName);
                final ReceiveMessageResponse response = sqsAsyncClient
                        .receiveMessage(ReceiveMessageRequest.builder()
                                .waitTimeSeconds(1)
                                .queueUrl(queueUrl)
                                .build())
                        .get();
                if (response.messages() != null) {
                    final List<Message> messages = response.messages();
                    messages.forEach(message -> {
                        acknowledge(message.receiptHandle());
                    });
                    return messages;
                } else {
                    return emptyList();
                }
        } catch (final Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public CompletableFuture<DeleteMessageResponse> acknowledge(final String receiptHandle) {
        return sqsAsyncClient.deleteMessage(DeleteMessageRequest
                .builder()
                .receiptHandle(receiptHandle)
                .build());
    }
}
