package de.otto.edison.eventsourcing.kinesis;

import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.*;

import java.util.List;

public class KinesisStreamSetupUtils {
    static boolean doesStreamExist(KinesisClient kinesisClient, String streamName, String from) {
        ListStreamsRequest.Builder builder = ListStreamsRequest.builder().exclusiveStartStreamName(from);
        if (from != null) {
            builder.exclusiveStartStreamName(from);
        }
        ListStreamsResponse listStreamsResponse = kinesisClient.listStreams(builder.build());
        List<String> streamNames = listStreamsResponse.streamNames();
        if (streamNames.stream().anyMatch(streamName::equals)) {
            return true;
        } else if (listStreamsResponse.hasMoreStreams()) {
            return doesStreamExist(kinesisClient, streamName, streamNames.get(streamNames.size()-1));
        }
        return false;
    }

    static void createStreamIfNotExists(KinesisClient kinesisClient, String streamName, int numberOfShards) {
        if (!doesStreamExist(kinesisClient, streamName, null)) {
            kinesisClient.createStream(CreateStreamRequest.builder()
                    .streamName(streamName)
                    .shardCount(numberOfShards)
                    .build());
            DescribeStreamResponse describeStreamResponse;
            do {
                describeStreamResponse = kinesisClient.describeStream(DescribeStreamRequest.builder()
                        .streamName(streamName)
                        .build());
            } while(! "ACTIVE".equals(describeStreamResponse.streamDescription().streamStatus()));
        }
    }
}
