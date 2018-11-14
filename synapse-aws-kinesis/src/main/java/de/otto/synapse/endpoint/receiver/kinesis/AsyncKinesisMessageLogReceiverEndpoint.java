package de.otto.synapse.endpoint.receiver.kinesis;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.receiver.AbstractMessageLogReceiverEndpoint;
import de.otto.synapse.message.Message;
import de.otto.synapse.message.kinesis.KinesisMessage;
import org.slf4j.Logger;
import org.springframework.context.ApplicationEventPublisher;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.*;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;

import static java.lang.Thread.sleep;
import static org.slf4j.LoggerFactory.getLogger;

public class AsyncKinesisMessageLogReceiverEndpoint extends AbstractMessageLogReceiverEndpoint {

    private static final Logger LOG = getLogger(AsyncKinesisMessageLogReceiverEndpoint.class);
    public static final String CONSUMER_NAME = "synapse-test-AsyncKinesisMessageLogReceiverEndpointTest";

    @Nonnull
    private final KinesisAsyncClient kinesisAsyncClient;

    public AsyncKinesisMessageLogReceiverEndpoint(final @Nonnull String channelName,
                                                  final @Nonnull MessageInterceptorRegistry interceptorRegistry,
                                                  final @Nonnull KinesisAsyncClient kinesisAsyncClient,
                                                  final @Nonnull ObjectMapper objectMapper,
                                                  final @Nullable ApplicationEventPublisher eventPublisher) {
        super(channelName, interceptorRegistry, objectMapper, eventPublisher);
        this.kinesisAsyncClient = kinesisAsyncClient;
    }

    @Nonnull
    @Override
    public CompletableFuture<ChannelPosition> consumeUntil(final @Nonnull ChannelPosition startFrom,
                                                           final @Nonnull Instant until) {
        final KinesisStreamInfo streamInfo = fetchKinesisStreamInfo();
        final String consumerArn = registerConsumerIfMissing(streamInfo.getArn());
        streamInfo.getShardInfo()
                .stream()
                .filter(KinesisShardInfo::isOpen)
                .forEach(shardInfo -> {
                    final SubscribeToShardRequest request = SubscribeToShardRequest.builder()
                            .consumerARN(consumerArn)
                            .shardId(shardInfo.getShardName())
                            .startingPosition(StartingPosition.builder().type(ShardIteratorType.TRIM_HORIZON).build())
                            .build();
                    SubscribeToShardResponseHandler responseHandler = SubscribeToShardResponseHandler
                            .builder()
                            .onError(t -> LOG.error("Error received during SubscribeToShard - {}", t.getMessage()))
                            .subscriber(e -> e.accept(SubscribeToShardResponseHandler.Visitor.builder()
                                    .onSubscribeToShardEvent(event -> {
                                        LOG.info("Received event - {}", e);
                                        event.records().forEach(record -> {
                                            Message<String> message = KinesisMessage.kinesisMessage(shardInfo.getShardName(), record);
                                            getMessageDispatcher().accept(message);
                                        });
                                    }).build()))
                            .build();
                    LOG.info("Subscribing to shard {}", shardInfo.getShardName());
                    kinesisAsyncClient.subscribeToShard(request, responseHandler);
                    LOG.info("Waiting after SubscribeToShard {} to become available again...", shardInfo.getShardName());
                    try {
                        sleep(2000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    LOG.info("...done.");
                });

        return null;
    }

    private String registerConsumerIfMissing(final String streamARN) {
        final String consumerArn;
        final DescribeStreamConsumerResponse describeStreamConsumerResponse = describeStreamConsumer(streamARN);
        if (describeStreamConsumerResponse == null) {
            consumerArn = registerConsumer(streamARN);
        } else {
            consumerArn = describeStreamConsumerResponse.consumerDescription().consumerARN();
        }
        return consumerArn;
    }

    private String registerConsumer(final String streamARN) {


        // TODO: fetch consumerName from somewhere...


        LOG.info("Registering consumer at stream ARN {}", streamARN);
        kinesisAsyncClient
                .registerStreamConsumer(RegisterStreamConsumerRequest
                        .builder()
                        .consumerName(CONSUMER_NAME)
                        .streamARN(streamARN)
                        .build())
                .join();
        return waitForConsumerUntilActive(streamARN);
    }

    private String waitForConsumerUntilActive(final String streamArn) {
        boolean isActive = false;
        String consumerARN = null;
        do {
            try {
                sleep(1000);
            } catch (InterruptedException e) {
                LOG.error(e.getMessage(), e);
                throw new RuntimeException(e.getMessage(), e);
            }
            LOG.info("Waiting for consumer to become active...");
            ConsumerDescription consumerDescription = describeStreamConsumer(streamArn).consumerDescription();
            consumerARN = consumerDescription.consumerARN();
            isActive = consumerDescription.consumerStatus() == ConsumerStatus.ACTIVE;
        } while (!isActive);
        LOG.info("Successfully registered consumer at stream ARN {}", streamArn);
        return consumerARN;
    }

    private DescribeStreamConsumerResponse describeStreamConsumer(final String streamARN) {
        return kinesisAsyncClient
                .describeStreamConsumer(DescribeStreamConsumerRequest.builder()
                        .consumerName(CONSUMER_NAME)
                        .streamARN(streamARN)
                        .build())
                .exceptionally((throwable -> null))
                .join();
    }

    private KinesisStreamInfo fetchKinesisStreamInfo() {
        return new KinesisStreamInfoProvider(kinesisAsyncClient)
                .getStreamInfo(getChannelName());
    }

    @Override
    public void stop() {

    }
}
