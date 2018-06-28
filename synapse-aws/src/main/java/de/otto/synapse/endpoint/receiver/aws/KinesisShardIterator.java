package de.otto.synapse.endpoint.receiver.aws;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import de.otto.synapse.channel.ShardPosition;
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
import software.amazon.awssdk.services.kinesis.model.*;

import java.util.concurrent.atomic.AtomicBoolean;

import static de.otto.synapse.channel.ShardPosition.fromHorizon;
import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static de.otto.synapse.logging.LogHelper.warn;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static software.amazon.awssdk.services.kinesis.model.ShardIteratorType.*;

/**
 * A helper class used to retrieve and traverse Kinesis Shards.
 * <p>
 *     <em>Caution:</em> Creating a KinesisShardIterator is an expensive operation, so instances should be
 *     reused and messages should be read continuously using by calling {@link #next()} should be preferred.
 *     Creating a KinesisShardIterator too often may result in a {@link ProvisionedThroughputExceededException}
 *     coming from the Amazon Kinesis SDK as described
 *     {@link KinesisClient#getShardIterator(GetShardIteratorRequest) here}.
 * </p>
 */
public class KinesisShardIterator {

    private static final Logger LOG = LoggerFactory.getLogger(KinesisShardIterator.class);

    public final static String POISON_SHARD_ITER = "__synapse__poison__iter";

    public static final Integer FETCH_RECORDS_LIMIT = 10000;
    private static final int RETRY_MAX_ATTEMPTS = 16;
    private static final int RETRY_BACK_OFF_POLICY_INITIAL_INTERVAL = 1000;
    private static final int RETRY_BACK_OFF_POLICY_MAX_INTERVAL = 64000;
    private static final double RETRY_BACK_OFF_POLICY_MULTIPLIER = 2.0;

    private final KinesisClient kinesisClient;
    private final String channelName;
    private String id;
    private ShardPosition shardPosition;
    private final int fetchRecordLimit;
    private final RetryTemplate retryTemplate;
    private final AtomicBoolean stopSignal = new AtomicBoolean(false);

    public KinesisShardIterator(final KinesisClient kinesisClient,
                                final String channelName,
                                final ShardPosition shardPosition) {
        this(kinesisClient, channelName, shardPosition, FETCH_RECORDS_LIMIT);
    }

    public KinesisShardIterator(final KinesisClient kinesisClient,
                                final String channelName,
                                final ShardPosition shardPosition,
                                final int fetchRecordLimit) {
        this.kinesisClient = kinesisClient;
        this.fetchRecordLimit = fetchRecordLimit;
        this.retryTemplate = createRetryTemplate();
        this.channelName = channelName;
        this.shardPosition = shardPosition;
        this.id = kinesisClient
                .getShardIterator(buildIteratorShardRequest(shardPosition))
                .shardIterator();
    }

    public String getId() {
        return this.id;
    }

    public ShardPosition getShardPosition() {
        return shardPosition;
    }

    public int getFetchRecordLimit() {
        return fetchRecordLimit;
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

    public KinesisShardResponse next() {
        try {
            final Stopwatch stopwatch = Stopwatch.createStarted();
            final GetRecordsResponse recordsResponse = retryTemplate.execute((RetryCallback<GetRecordsResponse, Throwable>) context -> {
                if (stopSignal.get()) {
                    context.setExhaustedOnly();
                }
                return tryNext();
            });
            return new KinesisShardResponse(channelName, shardPosition.shardName(), shardPosition, recordsResponse, stopwatch.elapsed(MILLISECONDS));
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private GetShardIteratorRequest buildIteratorShardRequest(final ShardPosition shardPosition) {
        final GetShardIteratorRequest.Builder shardRequestBuilder = GetShardIteratorRequest
                .builder()
                .shardId(shardPosition.shardName())
                .streamName(channelName);

        switch (shardPosition.startFrom()) {
            case HORIZON:
                shardRequestBuilder.shardIteratorType(TRIM_HORIZON);
                break;
            case POSITION:
                shardRequestBuilder.shardIteratorType(AFTER_SEQUENCE_NUMBER);
                shardRequestBuilder.startingSequenceNumber(shardPosition.position());
                break;
            case TIMESTAMP:
                shardRequestBuilder
                        .shardIteratorType(AT_TIMESTAMP)
                        .timestamp(shardPosition.timestamp());
                break;
        }
        return shardRequestBuilder.build();
    }

    private GetRecordsResponse tryNext() {
        GetRecordsResponse response = kinesisClient.getRecords(GetRecordsRequest.builder()
                .shardIterator(id)
                .limit(fetchRecordLimit)
                .build());
        this.id = response.nextShardIterator();
        LOG.debug("next() with id " + this.id + " returned " + response.records().size() + " records");
        if (!response.records().isEmpty()) {
            this.shardPosition = fromPosition(
                    shardPosition.shardName(),
                    response.records().get(response.records().size()-1).sequenceNumber()
            );
        }
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
