package de.otto.synapse.endpoint.receiver.kinesis;

import de.otto.synapse.channel.ShardPosition;
import de.otto.synapse.channel.ShardResponse;
import jakarta.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.ProvisionedThroughputExceededException;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static java.lang.String.format;
import static software.amazon.awssdk.services.kinesis.model.ShardIteratorType.*;

/**
 * A helper class used to retrieve and traverse Kinesis Shards.
 * <p>
 *     <em>Caution:</em> Creating a KinesisShardIterator is an expensive operation, so instances should be
 *     reused and messages should be read continuously using by calling {@link #next()} should be preferred.
 *     Creating a KinesisShardIterator too often may result in a {@link ProvisionedThroughputExceededException}
 *     coming from the Amazon Kinesis SDK as described
 *     {@link KinesisAsyncClient#getShardIterator(GetShardIteratorRequest) here}.
 * </p>
 */
public class KinesisShardIterator {

    private static final Logger LOG = LoggerFactory.getLogger(KinesisShardIterator.class);

    public static final String POISON_SHARD_ITER = "__synapse__poison__iter";
    public static final Integer DEFAULT_FETCH_RECORDS_LIMIT = 10_000;
    public static final Integer DEFAULT_FETCH_RECORDS_TIMEOUT_MILLIS = 60_000;
    private static final int MAX_RETRIES = 3;

    private final KinesisAsyncClient kinesisClient;
    private final String channelName;
    private final int fetchRecordsLimit;
    private final int fetchRecordsTimeout;
    private final AtomicBoolean stopSignal = new AtomicBoolean(false);
    private String id;
    private ShardPosition shardPosition;

    public KinesisShardIterator(final @Nonnull KinesisAsyncClient kinesisClient,
                                final @Nonnull String channelName,
                                final @Nonnull ShardPosition shardPosition) {
        this(kinesisClient, channelName, shardPosition, DEFAULT_FETCH_RECORDS_LIMIT, DEFAULT_FETCH_RECORDS_TIMEOUT_MILLIS);
    }

    public KinesisShardIterator(final @Nonnull KinesisAsyncClient kinesisClient,
                                final @Nonnull String channelName,
                                final @Nonnull ShardPosition shardPosition,
                                final int fetchRecordsLimit,
                                final int fetchRecordsTimeout) {
        this.kinesisClient = kinesisClient;
        this.fetchRecordsLimit = fetchRecordsLimit;
        this.fetchRecordsTimeout = fetchRecordsTimeout;
        this.channelName = channelName;
        this.shardPosition = shardPosition;
        this.id = createShardIteratorId();
    }

    private String createShardIteratorId() {
        return kinesisClient
                .getShardIterator(buildIteratorShardRequest(shardPosition))
                .join()
                .shardIterator();
    }

    public String getId() {
        return this.id;
    }

    @Nonnull
    public ShardPosition getShardPosition() {
        return shardPosition;
    }

    public int getFetchRecordsLimit() {
        return fetchRecordsLimit;
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

    public ShardResponse next() {
        if (!stopSignal.get()) {
            GetRecordsResponse recordsResponse = tryNextWithRetry();
            if (recordsResponse.records() == null || recordsResponse.records().size() == 0) {
                LOG.debug("GetRecordsResponse contains no records.");
            }
            return KinesisShardResponse.kinesisShardResponse(shardPosition, recordsResponse);
        } else {
            throw new IllegalStateException(format("Cannot iterate on shard '%s' after stop signal was received", shardPosition.shardName()));
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
            case AT_POSITION:
                shardRequestBuilder.shardIteratorType(AT_SEQUENCE_NUMBER);
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

    private GetRecordsResponse tryNextWithRetry() {
        RuntimeException exception = null;
        int retry = 0;
        while (retry++ < MAX_RETRIES) {
            try {
                return tryNext();
            } catch (RuntimeException e) {
                exception = e;
                LOG.warn(String.format("Failed to iterate on kinesis shard. Try to reset iterator and retry (%d/%d).", retry, MAX_RETRIES), e);
                id = createShardIteratorId();
            }
        }
        throw exception;
    }

    private GetRecordsResponse tryNext() {
        GetRecordsResponse response = null;
        try {
            response = kinesisClient.getRecords(GetRecordsRequest.builder()
                    .shardIterator(id)
                    .limit(fetchRecordsLimit)
                    .build())
                    .get(fetchRecordsTimeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | TimeoutException e) {
            LOG.warn("Timeout! GetRecords call in KinesisShardIterator did not return after {} milliseconds.", fetchRecordsTimeout, e);
            throw new RuntimeException("Timeout while executing getRecords", e);
        } catch (ExecutionException e) {
            throw new RuntimeException("ExecutionException during getRecords", e);
        }
        if (response.millisBehindLatest() == null) {
            throw new RuntimeException("millisBehindLatest inside a GetRecordsResponse was null. The response was: " + response);
        }
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

}
