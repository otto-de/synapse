package de.otto.synapse.sender.aws;

import de.otto.synapse.client.aws.RetryPutRecordsKinesisClient;
import de.otto.synapse.message.Message;
import de.otto.synapse.sender.MessageSender;
import de.otto.synapse.translator.MessageTranslator;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.collect.Lists.partition;

public class KinesisMessageSender implements MessageSender {

    private static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.allocateDirect(0);
    private static final int PUT_RECORDS_BATCH_SIZE = 500;

    private final String streamName;
    private final MessageTranslator<ByteBuffer> messageTranslator;
    private final RetryPutRecordsKinesisClient retryPutRecordsKinesisClient;

    public KinesisMessageSender(final String streamName,
                                final MessageTranslator<ByteBuffer> messageTranslator,
                                final KinesisClient kinesisClient) {
        this.streamName = streamName;
        this.messageTranslator = messageTranslator;
        this.retryPutRecordsKinesisClient = new RetryPutRecordsKinesisClient(kinesisClient);
    }

    @Override
    public <T> void send(final Message<T> message) {
        final Message<ByteBuffer> byteBufferMessage = messageTranslator.translate(message);
        PutRecordsRequestEntry putRecordsRequestEntry = requestEntryFor(message.getKey(), byteBufferMessage.getPayload());

        PutRecordsRequest putRecordsRequest = PutRecordsRequest.builder()
                .streamName(streamName)
                .records(putRecordsRequestEntry)
                .build();

        retryPutRecordsKinesisClient.putRecords(putRecordsRequest);
    }

    @Override
    public <T> void sendBatch(final Stream<Message<T>> messageStream) {
        List<PutRecordsRequestEntry> entries = messageStream.map(messageTranslator::translate)
                .map(entry -> requestEntryFor(entry.getKey(), entry.getPayload()))
                .collect(Collectors.toCollection(ArrayList::new));

        partition(entries, PUT_RECORDS_BATCH_SIZE)
                .forEach(batch -> {
                            PutRecordsRequest putRecordsRequest = PutRecordsRequest.builder()
                                    .streamName(streamName)
                                    .records(batch)
                                    .build();

                            retryPutRecordsKinesisClient.putRecords(putRecordsRequest);
                        }
                );
    }

    private PutRecordsRequestEntry requestEntryFor(final String key,
                                                   final ByteBuffer byteBuffer) {
        return PutRecordsRequestEntry.builder()
                .partitionKey(key)
                .data(byteBuffer != null ? byteBuffer : EMPTY_BYTE_BUFFER)
                .build();
    }
}
