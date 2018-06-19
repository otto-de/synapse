package de.otto.synapse.endpoint.receiver.aws;

import de.otto.synapse.channel.ShardPosition;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.endpoint.InterceptorChain;
import de.otto.synapse.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.ProvisionedThroughputExceededException;

import javax.annotation.Nullable;
import java.time.Clock;
import java.util.concurrent.atomic.AtomicReference;

public class KinesisPollingMessageLogReader {

    private static final Logger LOG = LoggerFactory.getLogger(KinesisPollingMessageLogReader.class);

    private final Clock clock = Clock.systemDefaultZone();
    private final InterceptorChain emptyInterceptorChain = new InterceptorChain();
    private final KinesisClient kinesisClient;

    public KinesisPollingMessageLogReader(final KinesisClient kinesisClient) {
        this.kinesisClient = kinesisClient;
    }

    /**
     * Reads a single {@link Message} from a Kinesis shard.
     * <p>
     *     The Message is the next message after the specified {@code shardPosition}
     * </p>
     * <p>
     *     <em>Caution:</em> By calling this method, a KinesisShardIterator is created, which is an expensive operation.
     *     Creating a KinesisShardIterator too often may result in a {@link ProvisionedThroughputExceededException}
     *     coming from the Amazon Kinesis SDK as described
     *     {@link KinesisClient#getShardIterator(GetShardIteratorRequest) here}. If calling this method results in
     *     {@code ProvisionedThroughputExceededExceptions}, you should wait for some time (one second is recommended)
     *     before trying it again, as Amazon is only allowing 5(!) GetShardIteratorRequests per second.
     * </p>
     */
    @Nullable
    public Message<String> fetchOne(final String channelName,
                                    final ShardPosition shardPosition) {
        // TODO: retries...
        final KinesisShardIterator kinesisShardIterator = new KinesisShardIterator(kinesisClient, channelName, shardPosition, 1);
        final KinesisShardReader shardReader = new KinesisShardReader(channelName, shardPosition.shardName(), kinesisClient, emptyInterceptorChain, clock);
        final AtomicReference<Message<String>> result = new AtomicReference<>();
        shardReader.consume(kinesisShardIterator, MessageConsumer.of(".*", String.class, result::set));
        return result.get();
    }

    private void waitDependingOnRetryStep(final int retryStep) {
        try {
            Thread.sleep((long)Math.pow(2, retryStep) * 1000);
        } catch (final InterruptedException e) {
            LOG.error("Error while waiting for retry to receive channel history", e);
        }
    }
}
