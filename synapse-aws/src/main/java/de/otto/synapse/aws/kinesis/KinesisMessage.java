package de.otto.synapse.aws.kinesis;

import de.otto.synapse.message.Message;
import software.amazon.awssdk.services.kinesis.model.Record;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.function.Function;

import static de.otto.synapse.message.Header.responseHeader;
import static java.nio.charset.StandardCharsets.UTF_8;

public class KinesisMessage<T> extends Message<T> {

    private static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.allocateDirect(0);

    private static final Function<ByteBuffer, String> BYTE_BUFFER_STRING = byteBuffer -> {
        if (byteBuffer == null || byteBuffer.equals(EMPTY_BYTE_BUFFER)) {
            return null;
        } else {
            return UTF_8.decode(byteBuffer).toString();
        }

    };

    public static Message<String> kinesisMessage(final Duration durationBehind,
                                                 final Record record) {
        return new KinesisMessage<>(record, durationBehind, BYTE_BUFFER_STRING);
    }

    public static <T> Message<T> kinesisMessage(final Record record,
                                                final Function<ByteBuffer, T> decoder) {
        return new KinesisMessage<>(
                record,
                null,
                decoder);
    }

    public static <T> Message<T> kinesisMessage(final Duration durationBehind,
                                                final Record record,
                                                final Function<ByteBuffer, T> decoder) {
        return new KinesisMessage<>(
                record,
                durationBehind,
                decoder);
    }

    private KinesisMessage(final Record record,
                           final Duration durationBehind,
                           final Function<ByteBuffer, T> decoder) {
        super(
                record.partitionKey(),
                responseHeader(
                        record.sequenceNumber(),
                        record.approximateArrivalTimestamp(),
                        durationBehind
                ),
                decoder.apply(record.data()));
    }

}
