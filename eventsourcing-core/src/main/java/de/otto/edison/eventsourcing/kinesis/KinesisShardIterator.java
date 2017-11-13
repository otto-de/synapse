package de.otto.edison.eventsourcing.kinesis;

import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;

public class KinesisShardIterator {

    static final int FETCH_RECORDS_LIMIT = 10000;

    private final KinesisClient kinesisClient;
    private String id;

    public KinesisShardIterator(KinesisClient kinesisClient, String firstId) {
        this.kinesisClient = kinesisClient;
        this.id = firstId;
    }

    public String getId() {
        return this.id;
    }

    public GetRecordsResponse next() {
        GetRecordsResponse response = kinesisClient.getRecords(GetRecordsRequest.builder()
                .shardIterator(id)
                .limit(FETCH_RECORDS_LIMIT)
                .build());
        this.id = response.nextShardIterator();
        return response;
    }
}
