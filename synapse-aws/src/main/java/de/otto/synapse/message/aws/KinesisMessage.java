package de.otto.synapse.message.aws;

import de.otto.synapse.message.Message;
import software.amazon.awssdk.services.kinesis.model.Record;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.function.Function;

import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static de.otto.synapse.message.Header.responseHeader;
import static de.otto.synapse.message.Message.message;
import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.charset.StandardCharsets.UTF_8;

public class KinesisMessage {

    private static final ByteBuffer EMPTY_BYTE_BUFFER = allocateDirect(0);

    private static final Function<ByteBuffer, String> BYTE_BUFFER_STRING = byteBuffer -> {
        if (byteBuffer == null || byteBuffer.equals(EMPTY_BYTE_BUFFER)) {
            return null;
        } else {
            return UTF_8.decode(byteBuffer).toString();
        }

    };

    public static Message<String> kinesisMessage(final @Nonnull String shard,
                                                 final @Nonnull Duration durationBehind,
                                                 final @Nonnull Record record) {
        return message(
                record.partitionKey(),
                responseHeader(
                        fromPosition(shard, record.sequenceNumber()),
                        record.approximateArrivalTimestamp(),
                        durationBehind
                ),
                BYTE_BUFFER_STRING.apply(record.data()));
    }

}
