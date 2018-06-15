package de.otto.synapse.endpoint.receiver.aws;

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

import java.util.concurrent.atomic.AtomicBoolean;

import static de.otto.synapse.logging.LogHelper.warn;

public class KinesisShardIterator {

    private static final Logger LOG = LoggerFactory.getLogger(KinesisShardIterator.class);

    public final static String POISON_SHARD_ITER = "__synapse__poison__iter";

    static final Integer FETCH_RECORDS_LIMIT = 10000;
    private static final int RETRY_MAX_ATTEMPTS = 16;
    private static final int RETRY_BACK_OFF_POLICY_INITIAL_INTERVAL = 1000;
    private static final int RETRY_BACK_OFF_POLICY_MAX_INTERVAL = 64000;
    private static final double RETRY_BACK_OFF_POLICY_MULTIPLIER = 2.0;

    private final KinesisClient kinesisClient;
    private String id;
    private final int fetchRecordLimit;
    private final RetryTemplate retryTemplate;
    private final AtomicBoolean stopSignal = new AtomicBoolean(false);

    public KinesisShardIterator(KinesisClient kinesisClient, String firstId) {
        this.kinesisClient = kinesisClient;
        this.id = firstId;
        this.fetchRecordLimit = FETCH_RECORDS_LIMIT;
        this.retryTemplate = createRetryTemplate();
    }

    public KinesisShardIterator(KinesisClient kinesisClient, String firstId, int fetchRecordLimit) {
        this.kinesisClient = kinesisClient;
        this.id = firstId;
        this.fetchRecordLimit = fetchRecordLimit;
        this.retryTemplate = createRetryTemplate();
    }

    public String getId() {
        return this.id;
    }

    /**
     * The shard iterator has returned an id that is matching {@link #POISON_SHARD_ITER}.
     * <p>
     *     !!!Only intended for testing purposes!!!
     * </p>
     * <p>
     *     This is useful in tests, when you want to finish consumption of Kinesis message logs after some
     *     mocked responses. Have a look at KinesisMessageLogReceiverEndpointTest for examples on how to use
     *     this.
     * </p>
     * @return true if the iterator is poisonous, false otherwise.
     */
    boolean isPoison() {
        return this.id.equals(POISON_SHARD_ITER);
    }

    public void stop() {
        stopSignal.set(true);
    }

    public GetRecordsResponse next() {
        try {
            return retryTemplate.execute((RetryCallback<GetRecordsResponse, Throwable>) context -> {
                if (stopSignal.get()) {
                    context.setExhaustedOnly();
                }
                return tryNext();
            });
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private GetRecordsResponse tryNext() {
        GetRecordsResponse response = kinesisClient.getRecords(GetRecordsRequest.builder()
                .shardIterator(id)
                .limit(fetchRecordLimit)
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

            warn(LOG, ImmutableMap.of("retryCount", context.getRetryCount(), "errorMessage", t.getMessage()), "fail to iterate on shard", null);
        }
    }

}
