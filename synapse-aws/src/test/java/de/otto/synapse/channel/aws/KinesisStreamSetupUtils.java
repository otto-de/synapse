package de.otto.synapse.channel.aws;

import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.*;

import java.util.List;

public class KinesisStreamSetupUtils {

    private static final int MAX_RETRIES = 10;

    static boolean doesStreamExist(KinesisClient kinesisClient, String channelName, String from) {
        ListStreamsRequest.Builder builder = ListStreamsRequest.builder().exclusiveStartStreamName(from);
        if (from != null) {
            builder.exclusiveStartStreamName(from);
        }
        ListStreamsResponse listStreamsResponse = kinesisClient.listStreams(builder.build());
        List<String> streamNames = listStreamsResponse.streamNames();
        if (streamNames.stream().anyMatch(channelName::equals)) {
            return true;
        } else if (listStreamsResponse.hasMoreStreams()) {
            return doesStreamExist(kinesisClient, channelName, streamNames.get(streamNames.size() - 1));
        }
        return false;
    }

    public static void createStreamIfNotExists(KinesisClient kinesisClient, String channelName, int numberOfShards) {
        if (!doesStreamExist(kinesisClient, channelName, null)) {
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
