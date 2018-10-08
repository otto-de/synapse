package de.otto.synapse.endpoint.sender.aws;

import de.otto.synapse.endpoint.sender.AbstractMessageSenderEndpoint;
import de.otto.synapse.message.Message;
import de.otto.synapse.translator.MessageTranslator;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static com.google.common.collect.Lists.partition;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.stream.Collectors.toCollection;

public class KinesisMessageSender extends AbstractMessageSenderEndpoint {

    private static final int PUT_RECORDS_BATCH_SIZE = 500;

    private final KinesisAsyncClient kinesisAsyncClient;

    public KinesisMessageSender(final String channelName,
                                final MessageTranslator<String> messageTranslator,
                                final KinesisAsyncClient kinesisClient) {
        super(channelName, messageTranslator);
        this.kinesisAsyncClient = kinesisClient;
    }

    @Override
    protected CompletableFuture<Void> doSend(@Nonnull Message<String> message) {
        // TODO: Introduce a response object and return it instead of Void
        // Just because we need a CompletableFuture<Void>, no CompletableFuture<SendMessageBatchResponse>:
        return allOf(kinesisAsyncClient.putRecords(createPutRecordRequest(message)));
    }

    @Override
    protected CompletableFuture<Void> doSendBatch(@Nonnull Stream<Message<String>> messageStream) {
        final List<PutRecordsRequestEntry> entries = createPutRecordRequestEntries(messageStream);
        return allOf(
                partition(entries, PUT_RECORDS_BATCH_SIZE)
                        .stream()
                        .map(batch -> kinesisAsyncClient.putRecords(createPutRecordRequest(batch)))
                        .toArray(CompletableFuture[]::new));
    }

    private PutRecordsRequest createPutRecordRequest(final List<PutRecordsRequestEntry> batch) {
        return PutRecordsRequest.builder()
                .streamName(getChannelName())
                .records(batch)
                .build();
    }

    private ArrayList<PutRecordsRequestEntry> createPutRecordRequestEntries(final @Nonnull Stream<Message<String>> messageStream) {
        return messageStream
                .map(entry -> requestEntryFor(entry.getKey(), entry.getPayload()))
                .collect(toCollection(ArrayList::new));
    }

    private PutRecordsRequestEntry requestEntryFor(final String key,
                                                   final String payload) {
        final SdkBytes sdkBytesPayload = payload != null
                ? SdkBytes.fromString(payload, UTF_8)
                : SdkBytes.fromByteArray(new byte[] {});
        return PutRecordsRequestEntry.builder()
                .partitionKey(key)
                .data(sdkBytesPayload)
                .build();
    }

    private PutRecordsRequest createPutRecordRequest(final @Nonnull Message<String> message) {
        return PutRecordsRequest.builder()
                .streamName(getChannelName())
                .records(requestEntryFor(message.getKey(), message.getPayload()))
                .build();
    }


}
