package de.otto.edison.eventsourcing.kinesis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;

public class KinesisShardIterator {

    private static final Logger LOG = LoggerFactory.getLogger(KinesisShardIterator.class);

    static final int FETCH_RECORDS_LIMIT = 10000;

    private final KinesisClient kinesisClient;
    private String id;
    private final RetryTemplate retryTemplate;

    public KinesisShardIterator(KinesisClient kinesisClient, String firstId) {
        this.kinesisClient = kinesisClient;
        this.id = firstId;
        this.retryTemplate = createRetryTemplate();
    }

    public String getId() {
        return this.id;
    }

    public GetRecordsResponse next() {
        try {
            return retryTemplate.execute((RetryCallback<GetRecordsResponse, Throwable>) context -> {
                try {
                    return tryNext();
                } catch (Exception e) {
                    LOG.info("failed to iterate on shard: {}", e.getMessage());
                    throw e;
                }
            });
        } catch (Throwable t) {
            throw new RuntimeException(t);
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

    private RetryTemplate createRetryTemplate() {
        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy();
        retryPolicy.setMaxAttempts(16);

        ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setInitialInterval(1000);
        backOffPolicy.setMaxInterval(60000);
        backOffPolicy.setMultiplier(2.0);

        RetryTemplate template = new RetryTemplate();
        template.setRetryPolicy(retryPolicy);
        template.setBackOffPolicy(backOffPolicy);

        return template;
    }

}
