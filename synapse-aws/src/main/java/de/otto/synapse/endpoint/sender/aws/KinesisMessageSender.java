package de.otto.synapse.endpoint.sender.aws;

import de.otto.synapse.client.aws.RetryPutRecordsKinesisClient;
import de.otto.synapse.endpoint.sender.AbstractMessageSenderEndpoint;
import de.otto.synapse.message.Message;
import de.otto.synapse.translator.MessageTranslator;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static com.google.common.collect.Lists.partition;
import static java.nio.ByteBuffer.wrap;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toCollection;

public class KinesisMessageSender extends AbstractMessageSenderEndpoint {

    private static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.allocateDirect(0);
    private static final int PUT_RECORDS_BATCH_SIZE = 500;

    private final RetryPutRecordsKinesisClient retryPutRecordsKinesisClient;

    public KinesisMessageSender(final String channelName,
                                final MessageTranslator<String> messageTranslator,
                                final KinesisClient kinesisClient) {
        super(channelName, messageTranslator);
        this.retryPutRecordsKinesisClient = new RetryPutRecordsKinesisClient(kinesisClient);
    }

    @Override
    protected void doSend(@Nonnull Message<String> message) {
        final PutRecordsRequest putRecordsRequest = PutRecordsRequest.builder()
                .streamName(getChannelName())
                .records(requestEntryFor(message.getKey(), message.getPayload()))
                .build();

        retryPutRecordsKinesisClient.putRecords(putRecordsRequest);
    }

    @Override
    protected void doSendBatch(@Nonnull Stream<Message<String>> messageStream) {
        final List<PutRecordsRequestEntry> entries = messageStream
                .map(entry -> requestEntryFor(entry.getKey(), entry.getPayload()))
                .collect(toCollection(ArrayList::new));
        partition(entries, PUT_RECORDS_BATCH_SIZE)
                .forEach(batch -> retryPutRecordsKinesisClient.putRecords(PutRecordsRequest.builder()
                        .streamName(getChannelName())
                        .records(batch)
                        .build())
                );
    }

    private PutRecordsRequestEntry requestEntryFor(final String key,
                                                   final String payload) {
        final ByteBuffer byteBufferPayload = payload != null
                ? wrap(payload.getBytes(UTF_8))
                : EMPTY_BYTE_BUFFER;
        return PutRecordsRequestEntry.builder()
                .partitionKey(key)
                .data(byteBufferPayload)
                .build();
    }
}
