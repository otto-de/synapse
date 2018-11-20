package de.otto.synapse.endpoint;

import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.net.MalformedURLException;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static de.otto.synapse.endpoint.sender.sqs.SqsMessageSender.MSG_KEY_ATTR;
import static java.time.Instant.now;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;

/**
 * A helper used to write tests for SQS senders or publishers. Not really recommended for production code.
 */
public class SqsClientHelper {

    private final SqsAsyncClient sqsAsyncClient;

    public SqsClientHelper(final SqsAsyncClient sqsAsyncClient) {
        this.sqsAsyncClient = sqsAsyncClient;
    }

    public boolean doesChannelExist(final String channelName) {
        try {
            final URL queueUrl = getQueueUrl(channelName);
            return getQueueUrls().contains(queueUrl);
        } catch (final RuntimeException e) {
            return false;
        }
    }

    public boolean doesChannelExist(final URL channelUrl) {
        try {
            return getQueueUrls().contains(channelUrl);
        } catch (final RuntimeException e) {
            return false;
        }
    }

    public List<URL> getQueueUrls() {
        try {
            final ListQueuesResponse queuesResponse = sqsAsyncClient.listQueues().get();
            return queuesResponse.queueUrls().stream().map(this::toUrl).collect(toList());
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public URL getQueueUrl(final String channelName) {
        try {
            return toUrl(sqsAsyncClient
                    .getQueueUrl(GetQueueUrlRequest.builder().queueName(channelName).build())
                    .get()
                    .queueUrl());
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

    public void purgeQueue(final String channelName) {
        try {
            if (doesChannelExist(channelName)) {
                final URL channelUrl = getQueueUrl(channelName);
                sqsAsyncClient.purgeQueue(PurgeQueueRequest
                        .builder()
                        .queueUrl(channelUrl.toString())
                        .build())
                        .get();
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public void purgeQueue(final URL channelUrl) {
        try {
            if (doesChannelExist(channelUrl)) {
                sqsAsyncClient.purgeQueue(PurgeQueueRequest
                        .builder()
                        .queueUrl(channelUrl.toString())
                        .build())
                        .get();
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public List<Message> receiveMessages(final String channelName,
                                         final int seconds) {
        final Instant started = now();
        List<Message> messages;
        boolean abort;
        do {
            messages = receiveMessages(channelName);
            abort = Duration.between(started, now()).getSeconds() > seconds;
        } while (messages.isEmpty() && !abort);
        return messages;
    }

    public List<Message> receiveMessages(final URL channelUrl,
                                         final int seconds) {
        final Instant started = now();
        List<Message> messages;
        boolean abort;
        do {
            messages = receiveMessages(channelUrl);
            abort = Duration.between(started, now()).getSeconds() > seconds;
        } while (messages.isEmpty() && !abort);
        return messages;
    }

    public List<Message> receiveMessages(final String queueName) {
        final URL channelUrl = getQueueUrl(queueName);
        return receiveMessages(channelUrl);
    }

    public List<Message> receiveMessages(final URL channelUrl) {
        try {
            final ReceiveMessageResponse response = sqsAsyncClient
                    .receiveMessage(ReceiveMessageRequest.builder()
                            .waitTimeSeconds(1)
                            .queueUrl(channelUrl.toString())
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

    public void sendMessage(final String channelName, final String key, final String payload) {
            final URL channelUrl = getQueueUrl(channelName);
            sendMessage(channelUrl, key, payload);
        try {
            final SendMessageResponse response = sqsAsyncClient.sendMessage(SendMessageRequest.builder()
                    .queueUrl(channelUrl.toString())
                    .messageAttributes(
                            singletonMap(MSG_KEY_ATTR, MessageAttributeValue.builder().dataType("String").stringValue(key).build()))
                    .messageBody(payload)
                    .build()
            ).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public void sendMessage(final URL channelUrl, final String key, final String payload) {
        try {
            final SendMessageResponse response = sqsAsyncClient.sendMessage(SendMessageRequest.builder()
                    .queueUrl(channelUrl.toString())
                    .messageAttributes(
                            singletonMap(MSG_KEY_ATTR, MessageAttributeValue.builder().dataType("String").stringValue(key).build()))
                    .messageBody(payload)
                    .build()
            ).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public CompletableFuture<DeleteMessageResponse> acknowledge(final String receiptHandle) {
        return sqsAsyncClient.deleteMessage(DeleteMessageRequest
                .builder()
                .receiptHandle(receiptHandle)
                .build());
    }

    public URL toUrl(final String url) {
        try {
            return new URL(url);
        } catch (MalformedURLException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
