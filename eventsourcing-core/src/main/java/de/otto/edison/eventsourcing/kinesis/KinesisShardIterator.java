package de.otto.edison.eventsourcing.kinesis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.KinesisException;

public class KinesisShardIterator {

    private static final Logger LOG = LoggerFactory.getLogger(KinesisShardIterator.class);

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

    @Retryable(
            value = KinesisException.class,
            backoff = @Backoff(delay = 500, maxDelay = 60000, multiplier = 2.0))
    public GetRecordsResponse next() {
        try {
            return tryNext();
        } catch (Exception e) {
            LOG.info("failed to iterate on shard: {}", e.getMessage());
            throw e;
        }
    }

    private GetRecordsResponse tryNext() {
        GetRecordsResponse response = kinesisClient.getRecords(GetRecordsRequest.builder()
                .shardIterator(id)
                .limit(FETCH_RECORDS_LIMIT)
                .build());
        this.id = response.nextShardIterator();
        return response;
    }
}
