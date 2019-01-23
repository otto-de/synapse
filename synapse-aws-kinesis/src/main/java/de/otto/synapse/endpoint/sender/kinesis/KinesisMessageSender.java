package de.otto.synapse.endpoint.sender.kinesis;

import com.google.common.collect.Lists;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.sender.AbstractMessageSenderEndpoint;
import de.otto.synapse.message.Message;
import de.otto.synapse.translator.MessageCodec;
import de.otto.synapse.translator.MessageFormat;
import de.otto.synapse.translator.MessageTranslator;
import org.slf4j.Logger;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toCollection;
import static org.slf4j.LoggerFactory.getLogger;

public class KinesisMessageSender extends AbstractMessageSenderEndpoint {

    private static final Logger LOG = getLogger(KinesisMessageSender.class);

    private static final int PUT_RECORDS_BATCH_SIZE = 500;
    private static final int MAX_RETRIES = 5;
    private static final long RETRY_DELAY_MS = 1000L;

    private final KinesisAsyncClient kinesisAsyncClient;
    private final MessageFormat messageFormat;

    public KinesisMessageSender(final String channelName,
                                final MessageInterceptorRegistry interceptorRegistry,
                                final MessageTranslator<String> messageTranslator,
                                final KinesisAsyncClient kinesisClient) {
        this(channelName, interceptorRegistry, messageTranslator, kinesisClient, MessageFormat.V1);
    }

    public KinesisMessageSender(final String channelName,
                                final MessageInterceptorRegistry interceptorRegistry,
                                final MessageTranslator<String> messageTranslator,
                                final KinesisAsyncClient kinesisClient,
                                final MessageFormat messageFormat) {
        super(channelName, interceptorRegistry, messageTranslator);
        this.kinesisAsyncClient = kinesisClient;
        this.messageFormat = messageFormat;
    }

    @Override
    protected CompletableFuture<Void> doSend(@Nonnull Message<String> message) {
        // TODO: Introduce a response object and return it instead of Void
        // Just because we need a CompletableFuture<Void>, no CompletableFuture<SendMessageBatchResponse>:
        return doSendBatch(Stream.of(message));
    }

    @Override
    protected CompletableFuture<Void> doSendBatch(@Nonnull Stream<Message<String>> messageStream) {
        final List<PutRecordsRequestEntry> entries = createPutRecordRequestEntries(messageStream);
        Lists.partition(entries, PUT_RECORDS_BATCH_SIZE)
                .forEach(this::blockingSendBatchWithRetries);
        return CompletableFuture.completedFuture(null);
    }

    private void blockingSendBatchWithRetries(List<PutRecordsRequestEntry> batch) {
        try {
            int currentRetry = 0;
            while (!blockingSendBatch(batch)) {
                currentRetry++;
                LOG.info("retry to send batch of size {} to kinesis for nth time: {}", batch.size(), currentRetry);
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
        try {
            kinesisAsyncClient.putRecords(createPutRecordsRequest(batch))
                    .thenApply(response -> {
                        isSuccessful.set(response.failedRecordCount() == 0);
                        return response;
                    }).get(2, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            LOG.warn("timeout while sending batch to kinesis", e);
            isSuccessful.set(false);
        }
        return isSuccessful.get();
    }

    private PutRecordsRequest createPutRecordsRequest(final List<PutRecordsRequestEntry> batch) {
        return PutRecordsRequest.builder()
                .streamName(getChannelName())
                .records(batch)
                .build();
    }

    private ArrayList<PutRecordsRequestEntry> createPutRecordRequestEntries(final @Nonnull Stream<Message<String>> messageStream) {
        return messageStream
                .map(this::requestEntryFor)
                .collect(toCollection(ArrayList::new));
    }

    private PutRecordsRequestEntry requestEntryFor(final Message<String> message) {
        final String encodedMessage = MessageCodec.encode(message, messageFormat);
        final SdkBytes sdkBytes = encodedMessage != null
                ? SdkBytes.fromString(encodedMessage, UTF_8)
                : SdkBytes.fromByteArray(new byte[]{});

        return PutRecordsRequestEntry.builder()
                .partitionKey(message.getKey().partitionKey())
                .data(sdkBytes)
                .build();
    }


}
