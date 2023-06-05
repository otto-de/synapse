package de.otto.synapse.endpoint.receiver.kinesis;

import de.otto.synapse.channel.ShardPosition;
import de.otto.synapse.channel.ShardResponse;
import de.otto.synapse.channel.StartFrom;
import de.otto.synapse.logging.LogHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.slf4j.Marker;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;

import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static de.otto.synapse.endpoint.receiver.kinesis.KinesisMessageLogReader.DEFAULT_WAITING_TIME_ON_EMPTY_RECORDS;


public class KinesisShardReader {
    private static final Logger LOG = LoggerFactory.getLogger(KinesisShardReader.class);
    public static final int LOG_MESSAGE_COUNTER_EVERY_NTH_MESSAGE = 1_000;

    private final String shardName;
    private final String channelName;
    private final KinesisAsyncClient kinesisClient;
    private final ExecutorService executorService;
    private final Clock clock;
    private final AtomicBoolean stopSignal = new AtomicBoolean(false);
    private final int waitingTimeOnEmptyRecords;
    private final Marker marker;

    public KinesisShardReader(final String channelName,
                              final String shardName,
                              final KinesisAsyncClient kinesisClient,
                              final ExecutorService executorService,
                              final Clock clock) {
        this(channelName, shardName, kinesisClient, executorService, clock, DEFAULT_WAITING_TIME_ON_EMPTY_RECORDS, null);
    }

    public KinesisShardReader(final String channelName,
                              final String shardName,
                              final KinesisAsyncClient kinesisClient,
                              final ExecutorService executorService,
                              final Clock clock,
                              final int waitingTimeOnEmptyRecords,
                              final Marker marker) {
        this.shardName = shardName;
        this.channelName = channelName;
        this.kinesisClient = kinesisClient;
        this.executorService = executorService;
        this.clock = clock;
        this.waitingTimeOnEmptyRecords = waitingTimeOnEmptyRecords;
        this.marker = marker;
    }

    public String getChannelName() {
        return channelName;
    }

    public String getShardName() {
        return shardName;
    }

    public CompletableFuture<ShardPosition> consumeUntil(final ShardPosition startFrom,
                                                         final Predicate<ShardResponse> stopCondition,
                                                         final Consumer<ShardResponse> responseConsumer) {
        final Map<String, String> copyOfContextMap = MDC.getCopyOfContextMap();
        return CompletableFuture.supplyAsync(() -> {
            if (copyOfContextMap != null) {
                MDC.setContextMap(copyOfContextMap);
            }
            MDC.put("channelName", channelName);
            MDC.put("shardName", shardName);
            LOG.info(marker, "Reading from channel={}, shard={}, position={}", channelName, shardName, startFrom);
            try {
                final long firstMessageLogTime = System.currentTimeMillis();
                final AtomicLong shardMessagesCounter = new AtomicLong(0);
                final AtomicLong previousMessageLogTime = new AtomicLong(System.currentTimeMillis());
                final AtomicLong previousLoggedMessageCounterMod = new AtomicLong(0), previousLoggedMessageCounter = new AtomicLong(0);
                final ShardPosition sanitizedShardPosition = sanitizePositionedShardPosition(startFrom);
                final KinesisShardIterator kinesisShardIterator = new KinesisShardIterator(kinesisClient, channelName, sanitizedShardPosition);

                boolean stopRetrieval;
                do {
                    /*
                    Poison-Pill injected by a test. This is helpful, if you want to write tests that should terminate
                    after a number of iterated shards.
                     */
                    if (kinesisShardIterator.isPoison()) {
                        LOG.warn(marker, "Received Poison-Pill - This should only happen during tests!");
                        break;
                    }

                    final ShardResponse response = kinesisShardIterator.next();
                    responseConsumer.accept(response);

                    int responseMessagesCounter = response.getMessages().size();
                    long totalMessagesCounter = shardMessagesCounter.addAndGet(responseMessagesCounter);

                    boolean stopConditionFulfilled = stopCondition.test(response);
                    stopRetrieval = stopConditionFulfilled || isStopping() || waitABit(response.getDurationBehind());

                    if ((totalMessagesCounter > 0 && totalMessagesCounter > previousLoggedMessageCounterMod.get() + LOG_MESSAGE_COUNTER_EVERY_NTH_MESSAGE) || stopRetrieval) {
                        double messagesPerSecond = LogHelper.calculateMessagesPerSecond(previousMessageLogTime.getAndSet(System.currentTimeMillis()), totalMessagesCounter - previousLoggedMessageCounter.get());

                        LOG.info(marker, "Read {} messages ({} per sec) from '{}:{}', durationBehind={}, totalMessages={}", responseMessagesCounter, String.format("%.2f", messagesPerSecond), channelName, shardName, response.getDurationBehind(), totalMessagesCounter);
                        if (stopRetrieval) {
                            LOG.info(marker, "Stop reading of channel={}, shard={}, stopCondition={}, stopSignal={}, durationBehind={}", channelName, shardName, stopConditionFulfilled, isStopping(), response.getDurationBehind());
                        }

                        previousLoggedMessageCounterMod.set(totalMessagesCounter - (totalMessagesCounter % LOG_MESSAGE_COUNTER_EVERY_NTH_MESSAGE));
                        previousLoggedMessageCounter.set(totalMessagesCounter);
                    }

                } while (!stopRetrieval);

                double totalMessagesPerSecond = LogHelper.calculateMessagesPerSecond(firstMessageLogTime, shardMessagesCounter.get());
                LOG.info(marker, "Read a total of {} messages from '{}:{}', totalMessagesPerSecond={}", shardMessagesCounter.get(), channelName, shardName, String.format("%.2f", totalMessagesPerSecond));

                return kinesisShardIterator.getShardPosition();

            } catch (final RuntimeException e) {
                LOG.error(marker, "Failed to consume from Kinesis shard {}: {}, {}", channelName, shardName, e.getMessage());
                // Stop all shards and shutdown if this shard is failing:
                stop();
                throw e;
            } finally {
                MDC.remove("channelName");
                MDC.remove("shardName");
            }
        }, executorService);
    }

    private ShardPosition sanitizePositionedShardPosition(ShardPosition shardPosition) {
        try {
            StartFrom startFrom = shardPosition.startFrom();

            if (startFrom == StartFrom.AT_POSITION || startFrom == StartFrom.POSITION) {
                ShardIteratorType type = startFrom == StartFrom.POSITION
                        ? ShardIteratorType.AFTER_SEQUENCE_NUMBER
                        : ShardIteratorType.AT_SEQUENCE_NUMBER;
                kinesisClient.getShardIterator(GetShardIteratorRequest.builder()
                        .shardId(shardName)
                        .streamName(channelName)
                        .shardIteratorType(type)
                        .startingSequenceNumber(shardPosition.position())
                        .build())
                        .get();
            }

            return shardPosition;
        } catch (ExecutionException | InterruptedException | RuntimeException e) {
            LOG.warn(marker, "given shardposition {} / {} not accessible, falling back to horizon", shardPosition.shardName(), shardPosition.position());
        }
        return ShardPosition.fromHorizon(shardPosition.shardName());
    }

    private boolean waitABit(Duration durationBehind) {
        try {
            /*Wait one second as documented by amazon: https://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetRecords.html*/
            if (durationBehind.getSeconds() > 10) {
                Thread.sleep(1000);
            } else {
                Thread.sleep(waitingTimeOnEmptyRecords);
            }
        } catch (final InterruptedException e) {
            LOG.warn(marker, "Thread got interrupted");
            return true;
        }
        return false;
    }

    public void stop() {
        LOG.info(marker, "Shard {} received stop signal.", shardName);
        stopSignal.set(true);
    }

    public boolean isStopping() {
        return stopSignal.get();
    }
}
