package de.otto.edison.eventsourcing.kinesis;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.listener.RetryListenerSupport;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.KinesisException;

public class KinesisShardIterator {

    private static final Logger LOG = LoggerFactory.getLogger(KinesisShardIterator.class);

    static final int FETCH_RECORDS_LIMIT = 10000;
    private static final int RETRY_MAX_ATTEMPTS = 16;
    private static final int RETRY_BACK_OFF_POLICY_INITIAL_INTERVAL = 1000;
    private static final int RETRY_BACK_OFF_POLICY_MAX_INTERVAL = 64000;
    private static final double RETRY_BACK_OFF_POLICY_MULTIPLIER = 2.0;

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
            return retryTemplate.execute((RetryCallback<GetRecordsResponse, Throwable>) context -> tryNext());
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
        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy(
                RETRY_MAX_ATTEMPTS,
                ImmutableMap.of(KinesisException.class, true,
                        SdkClientException.class, true));

        ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setInitialInterval(RETRY_BACK_OFF_POLICY_INITIAL_INTERVAL);
        backOffPolicy.setMaxInterval(RETRY_BACK_OFF_POLICY_MAX_INTERVAL);
        backOffPolicy.setMultiplier(RETRY_BACK_OFF_POLICY_MULTIPLIER);

        RetryTemplate template = new RetryTemplate();
        template.registerListener(new LogRetryListener());
        template.setRetryPolicy(retryPolicy);
        template.setBackOffPolicy(backOffPolicy);

        return template;
    }

    class LogRetryListener extends RetryListenerSupport {
        @Override
        public <T, E extends Throwable> void onError(RetryContext context, RetryCallback<T, E> callback, Throwable t) {
            LOG.info("{}. fail to iterate on shard: {}", context.getRetryCount(), t.getMessage());
        }
    }

}
