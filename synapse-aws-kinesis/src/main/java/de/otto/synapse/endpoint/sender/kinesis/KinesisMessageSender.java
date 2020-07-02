package de.otto.synapse.endpoint.sender.kinesis;

import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.sender.AbstractMessageSenderEndpoint;
import de.otto.synapse.message.TextMessage;
import de.otto.synapse.translator.MessageFormat;
import de.otto.synapse.translator.MessageTranslator;
import org.slf4j.Logger;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static de.otto.synapse.translator.MessageFormat.defaultMessageFormat;
import static java.util.stream.Collectors.toCollection;
import static org.slf4j.LoggerFactory.getLogger;

public class KinesisMessageSender extends AbstractMessageSenderEndpoint {

    private static final Logger LOG = getLogger(KinesisMessageSender.class);

    private static final int PUT_RECORDS_BATCH_SIZE = 500;
    private static final int PUT_RECORDS_BATCH_SIZE_BYTES = 5 * 1024 * 1024;
    private static final int MAX_RETRIES = 15;
    private static final long RETRY_DELAY_MS = 1000L;

    private final KinesisAsyncClient kinesisAsyncClient;
    private final KinesisEncoder encoder;
    private final MessageFormat messageFormat;

    public KinesisMessageSender(final String channelName,
                                final MessageInterceptorRegistry interceptorRegistry,
                                final MessageTranslator<TextMessage> messageTranslator,
                                final KinesisAsyncClient kinesisClient) {
        this(channelName, interceptorRegistry, messageTranslator, kinesisClient, defaultMessageFormat());
    }

    public KinesisMessageSender(final String channelName,
                                final MessageInterceptorRegistry interceptorRegistry,
                                final MessageTranslator<TextMessage> messageTranslator,
                                final KinesisAsyncClient kinesisClient,
                                final MessageFormat messageFormat) {
        super(channelName, interceptorRegistry, messageTranslator);
        this.kinesisAsyncClient = kinesisClient;
        this.encoder = new KinesisEncoder(messageFormat);
        this.messageFormat = messageFormat;
    }

    @Override
    protected CompletableFuture<Void> doSend(@Nonnull TextMessage message) {
        // TODO: Introduce a response object and return it instead of Void
        // Just because we need a CompletableFuture<Void>, no CompletableFuture<SendMessageBatchResponse>:
        return doSendBatch(Stream.of(message));
    }

    @Override
    protected CompletableFuture<Void> doSendBatch(@Nonnull Stream<TextMessage> messageStream) {
        // TODO: Introduce a response object and return it instead of Void
        // Just because we need a CompletableFuture<Void>, no CompletableFuture<SendMessageBatchResponse>:
        final List<PutRecordsRequestEntry> entries = createPutRecordRequestEntries(messageStream);
        long currentBatchSize = 0;
        List<PutRecordsRequestEntry> batch = new ArrayList<>();
        if (!entries.isEmpty()) {
            for (PutRecordsRequestEntry entry : entries) {
                currentBatchSize += getApproximateDataSize(entry);
                if (currentBatchSize > PUT_RECORDS_BATCH_SIZE_BYTES || batch.size() + 1 > PUT_RECORDS_BATCH_SIZE) {
                    blockingSendBatchWithRetries(batch);
                    batch = new ArrayList<>();
                    currentBatchSize = 0;
                }
                batch.add(entry);
            }
            if (!batch.isEmpty()) {
                blockingSendBatchWithRetries(batch);
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    private int getApproximateDataSize(PutRecordsRequestEntry entry) {
        return entry.data().asByteBuffer().limit() + entry.partitionKey().getBytes().length;
    }

    @Override
    public MessageFormat getMessageFormat() {
        return messageFormat;
    }

    private void blockingSendBatchWithRetries(List<PutRecordsRequestEntry> batch) {
        try {
            int currentRetry = 0;
            while (!blockingSendBatch(batch)) {
                currentRetry++;
                LOG.warn("retry to send batch of size '{}' to kinesis for nth time: {}", batch.size(), currentRetry);
                if (currentRetry >= MAX_RETRIES) {
                    throw new RetryLimitExceededException("Exceeded maximum number of retries.", MAX_RETRIES);
                }
                Thread.sleep(RETRY_DELAY_MS);
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean blockingSendBatch(List<PutRecordsRequestEntry> batch) throws ExecutionException, InterruptedException {
        AtomicBoolean isSuccessful = new AtomicBoolean(true);
        PutRecordsRequest putRecordsRequest = createPutRecordsRequest(batch);
        kinesisAsyncClient.putRecords(putRecordsRequest)
                .thenApply(response -> {
                    isSuccessful.set(response.failedRecordCount() == 0);
                    return response;
                }).get();
        return isSuccessful.get();
    }

    private PutRecordsRequest createPutRecordsRequest(final List<PutRecordsRequestEntry> batch) {
        return PutRecordsRequest.builder()
                .streamName(getChannelName())
                .records(batch)
                .build();
    }

    private ArrayList<PutRecordsRequestEntry> createPutRecordRequestEntries(final @Nonnull Stream<TextMessage> messageStream) {
        return messageStream
                .map(encoder::apply)
                .collect(toCollection(ArrayList::new));
    }

}
