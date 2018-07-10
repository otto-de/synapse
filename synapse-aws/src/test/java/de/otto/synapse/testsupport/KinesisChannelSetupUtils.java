package de.otto.synapse.testsupport;

import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.*;

import java.util.List;

public class KinesisChannelSetupUtils {

    private static final int MAX_RETRIES = 10;

    static boolean doesChannelExist(KinesisClient kinesisClient, String channelName, String from) {
        ListStreamsRequest.Builder builder = ListStreamsRequest.builder().exclusiveStartStreamName(from);
        if (from != null) {
            builder.exclusiveStartStreamName(from);
        }
        ListStreamsResponse listStreamsResponse = kinesisClient.listStreams(builder.build());
        List<String> streamNames = listStreamsResponse.streamNames();
        if (streamNames.stream().anyMatch(channelName::equals)) {
            return true;
        } else if (listStreamsResponse.hasMoreStreams()) {
            return doesChannelExist(kinesisClient, channelName, streamNames.get(streamNames.size() - 1));
        }
        return false;
    }

    public static void createChannelIfNotExists(KinesisClient kinesisClient, String channelName, int numberOfShards) {
        if (!doesChannelExist(kinesisClient, channelName, null)) {
            kinesisClient.createStream(CreateStreamRequest.builder()
                    .streamName(channelName)
                    .shardCount(numberOfShards)
                    .build());
            DescribeStreamResponse describeStreamResponse;
            int retries = 0;
            do {
                describeStreamResponse = kinesisClient.describeStream(DescribeStreamRequest.builder()
                        .streamName(channelName)
                        .build());
                
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
