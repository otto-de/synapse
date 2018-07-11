package de.otto.synapse.testsupport;

import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.*;
import software.amazon.awssdk.services.sqs.SQSAsyncClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.ListQueuesResponse;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class SqsChannelSetupUtils {

    static boolean doesChannelExist(SQSAsyncClient sqsAsyncClient, String channelName) {
        try {
            final String queueUrl = getQueueUrl(sqsAsyncClient, channelName);
            final ListQueuesResponse futureQueues = sqsAsyncClient.listQueues().get();
            return futureQueues.queueUrls().contains(queueUrl);

        } catch (InterruptedException | ExecutionException e) {
            if (e.getCause() instanceof QueueDoesNotExistException) {
                return false;
            } else {
                throw new RuntimeException(e.getMessage(), e);
            }
        }
    }

    public static String getQueueUrl(SQSAsyncClient sqsAsyncClient, String channelName) throws InterruptedException, ExecutionException {
        return sqsAsyncClient
                .getQueueUrl(GetQueueUrlRequest.builder().queueName(channelName).build())
                .get()
                .queueUrl();
    }

    public static void createChannelIfNotExists(final SQSAsyncClient sqsAsyncClient,
                                                final String channelName) {
        try {
            if (!doesChannelExist(sqsAsyncClient, channelName)) {
                sqsAsyncClient
                        .createQueue(CreateQueueRequest.builder()
                                .queueName(channelName)
                                .build()).get();
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
