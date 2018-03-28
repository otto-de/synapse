package de.otto.synapse.message.aws;

import de.otto.synapse.message.Message;
import software.amazon.awssdk.services.kinesis.model.Record;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.function.Function;

import static de.otto.synapse.channel.ChannelPosition.of;
import static de.otto.synapse.message.Header.responseHeader;
import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.charset.StandardCharsets.UTF_8;

public class KinesisMessage<T> extends Message<T> {

    private static final ByteBuffer EMPTY_BYTE_BUFFER = allocateDirect(0);

    private static final Function<ByteBuffer, String> BYTE_BUFFER_STRING = byteBuffer -> {
        if (byteBuffer == null || byteBuffer.equals(EMPTY_BYTE_BUFFER)) {
            return null;
        } else {
            return UTF_8.decode(byteBuffer).toString();
        }

    };

    public static Message<String> kinesisMessage(final String shard,
                                                 final Duration durationBehind,
                                                 final Record record) {
        if (record == null) {
            return new KinesisMessage<>(shard, durationBehind, BYTE_BUFFER_STRING);
        } else {
            return new KinesisMessage<>(shard, durationBehind, record, BYTE_BUFFER_STRING);
        }
    }

    private KinesisMessage(final String shard,
                           final Duration durationBehind,
                           final Record record,
                           final Function<ByteBuffer, T> decoder) {
        super(
                record.partitionKey(),
                responseHeader(
                        of(shard, record.sequenceNumber()),
                        record.approximateArrivalTimestamp(),
                        durationBehind
                ),
                decoder.apply(record.data()));
    }

    private KinesisMessage(final String shard,
                           final Duration durationBehind,
                           final Function<ByteBuffer, T> decoder) {
        super(
                "no_key",
                responseHeader(
                        of(shard, "unknown"),
                        Instant.MIN,
                        durationBehind
                ),
                decoder.apply(ByteBuffer.wrap(new byte[0])));
    }

}
