package de.otto.synapse.testsupport;

import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.*;

import java.util.List;

public class KinesisChannelSetupUtils {

    private static final int MAX_RETRIES = 10;

    static boolean doesChannelExist(final KinesisAsyncClient kinesisClient,
                                    final String channelName,
                                    final String from) {

        final ListStreamsResponse response = kinesisClient
                .listStreams(ListStreamsRequest.builder().exclusiveStartStreamName(from).build())
                .join();
        final List<String> streamNames = response.streamNames();
        if (streamNames.stream().anyMatch(channelName::equals)) {
            return true;
        } else if (response.hasMoreStreams()) {
            return doesChannelExist(kinesisClient, channelName, streamNames.get(streamNames.size() - 1));
        }
        return false;
    }

    public static void createChannelIfNotExists(final KinesisAsyncClient kinesisClient,
                                                final String channelName,
                                                final int numberOfShards) {
        if (!doesChannelExist(kinesisClient, channelName, null)) {
            kinesisClient.createStream(CreateStreamRequest.builder()
                    .streamName(channelName)
                    .shardCount(numberOfShards)
                    .build())
                    .join();
            DescribeStreamResponse describeStreamResponse;
            int retries = 0;
            do {
                describeStreamResponse = kinesisClient.describeStream(DescribeStreamRequest.builder()
                        .streamName(channelName)
                        .build())
                        .join();
                
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                retries++;
            } while ((StreamStatus.ACTIVE != describeStreamResponse.streamDescription().streamStatus()) && retries < MAX_RETRIES);

            if (describeStreamResponse.streamDescription().streamStatus() != StreamStatus.ACTIVE) {
                throw new RuntimeException(String.format("Timeout while waiting to create stream %s", channelName));
            }
        }
    }
}
