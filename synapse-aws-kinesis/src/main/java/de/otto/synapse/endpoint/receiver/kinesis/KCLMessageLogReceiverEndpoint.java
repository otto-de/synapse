package de.otto.synapse.endpoint.receiver.kinesis;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.channel.ChannelDurationBehind;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.configuration.aws.AwsProperties;
import de.otto.synapse.endpoint.InterceptorChain;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.receiver.AbstractMessageLogReceiverEndpoint;
import de.otto.synapse.message.Message;
import org.slf4j.Logger;
import org.springframework.context.ApplicationEventPublisher;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.checkpoint.CheckpointConfig;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.lifecycle.events.*;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;
import software.amazon.kinesis.retrieval.fanout.FanOutConfig;
import software.amazon.kinesis.retrieval.polling.PollingConfig;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import static de.otto.synapse.channel.ChannelDurationBehind.copyOf;
import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static de.otto.synapse.endpoint.EndpointType.RECEIVER;
import static de.otto.synapse.info.MessageReceiverNotification.builder;
import static de.otto.synapse.info.MessageReceiverStatus.RUNNING;
import static de.otto.synapse.message.Header.responseHeader;
import static de.otto.synapse.message.Message.message;
import static java.time.Duration.ofMillis;
import static org.slf4j.LoggerFactory.getLogger;

public class KCLMessageLogReceiverEndpoint extends AbstractMessageLogReceiverEndpoint {
    private static final Logger LOG = getLogger(KCLMessageLogReceiverEndpoint.class);
    private final AtomicReference<ChannelDurationBehind> channelDurationBehind = new AtomicReference<>();

    @Nonnull
    private final String channelName;
    @Nonnull
    private final MessageInterceptorRegistry interceptorRegistry;
    @Nullable
    private final ApplicationEventPublisher eventPublisher;
    private final KinesisAsyncClient kinesisClient;
    private final AwsProperties awsProperties;
    private Scheduler scheduler;

    public KCLMessageLogReceiverEndpoint(@Nonnull String channelName,
                                         @Nonnull MessageInterceptorRegistry interceptorRegistry,
                                         @Nonnull ObjectMapper objectMapper,
                                         @Nullable ApplicationEventPublisher eventPublisher,
                                         KinesisAsyncClient kinesisClient,
                                         AwsProperties awsProperties) {
        super(channelName, interceptorRegistry, objectMapper, eventPublisher);
        this.channelName = channelName;
        this.interceptorRegistry = interceptorRegistry;
        this.eventPublisher = eventPublisher;
        this.kinesisClient = kinesisClient;
        this.awsProperties = awsProperties;
    }

    @Nonnull
    @Override
    public CompletableFuture<ChannelPosition> consumeUntil(@Nonnull ChannelPosition startFrom, @Nonnull Instant until) {
        scheduler = initializeKCL();
        Thread schedulerThread = new Thread(scheduler);
        schedulerThread.setDaemon(true);
        schedulerThread.start();

        try {
            Thread.sleep(2000000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return CompletableFuture.completedFuture(startFrom);
    }

    @Override
    public void stop() {
        if (scheduler != null) {
            Future<Boolean> gracefulShutdownFuture = scheduler.startGracefulShutdown();
            LOG.info("Waiting up to 20 seconds for shutdown to complete.");
            try {
                gracefulShutdownFuture.get(20, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                LOG.info("Interrupted while waiting for graceful shutdown. Continuing.");
            } catch (ExecutionException e) {
                LOG.error("Exception while executing graceful shutdown.", e);
            } catch (TimeoutException e) {
                LOG.error("Timeout while waiting for shutdown.  Scheduler may not have exited.");
            }
        }
        LOG.info("Completed, shutting down now.");
    }

    private Scheduler initializeKCL() {
        DynamoDbAsyncClient dynamoClient = DynamoDbAsyncClient.builder().region(Region.of(awsProperties.getRegion())).build();
        CloudWatchAsyncClient cloudWatchClient = CloudWatchAsyncClient.builder().region(Region.of(awsProperties.getRegion())).build();
        ConfigsBuilder configsBuilder = new ConfigsBuilder(channelName, channelName, kinesisClient,
                dynamoClient, cloudWatchClient, UUID.randomUUID().toString(), new SynapseShardRecordProcessorFactory());

        Scheduler scheduler = new Scheduler(
                configsBuilder.checkpointConfig(),
                configsBuilder.coordinatorConfig(),
                configsBuilder.leaseManagementConfig(),
                configsBuilder.lifecycleConfig(),
                configsBuilder.metricsConfig(),
                configsBuilder.processorConfig(),
                configsBuilder.retrievalConfig().retrievalSpecificConfig(new FanOutConfig(kinesisClient))

        );
        return scheduler;
    }


    public class SynapseShardRecordProcessorFactory implements ShardRecordProcessorFactory {

        @Override
        public ShardRecordProcessor shardRecordProcessor() {
            return new SynapseShardRecordProcessor();
        }
    }

    public class SynapseShardRecordProcessor implements ShardRecordProcessor {
        private String shardId;

        @Override
        public void initialize(InitializationInput initializationInput) {
            shardId = initializationInput.shardId();
        }

        @Override
        public void processRecords(ProcessRecordsInput processRecordsInput) {
            final InterceptorChain interceptorChain = interceptorRegistry.getInterceptorChain(channelName, RECEIVER);
            processRecordsInput.records().forEach(record -> {

                Message<String> message = message(
                        record.partitionKey(),
                        responseHeader(
                                fromPosition(shardId, record.sequenceNumber()),
                                record.approximateArrivalTimestamp()
                        ),
                        record.data().toString());

                try {
                    final Message<String> interceptedMessage = interceptorChain.intercept(message);
                    if (interceptedMessage != null) {
                        getMessageDispatcher().accept(message);
                    }
                } catch (final Exception e) {
                    LOG.error("Error processing message: " + e.getMessage(), e);
                }
            });
            channelDurationBehind.updateAndGet(behind -> copyOf(behind)
                    .with(shardId, ofMillis(processRecordsInput.millisBehindLatest()))
                    .build());

            if (eventPublisher != null) {
                eventPublisher.publishEvent(builder()
                        .withChannelName(channelName)
                        .withChannelDurationBehind(channelDurationBehind.get())
                        .withStatus(RUNNING)
                        .withMessage("Reading from kinesis shard.")
                        .build());
            }

        }

        @Override
        public void leaseLost(LeaseLostInput leaseLostInput) {

        }

        @Override
        public void shardEnded(ShardEndedInput shardEndedInput) {

        }

        @Override
        public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput) {

        }
    }
}
