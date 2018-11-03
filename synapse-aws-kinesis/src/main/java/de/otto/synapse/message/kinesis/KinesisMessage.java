package de.otto.synapse.message.kinesis;

import de.otto.synapse.message.Message;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.model.Record;

import javax.annotation.Nonnull;
import java.util.function.Function;

import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static de.otto.synapse.message.Header.responseHeader;
import static de.otto.synapse.message.Message.message;
import static java.nio.charset.StandardCharsets.UTF_8;

public class KinesisMessage {

    private static final SdkBytes EMPTY_SDK_BYTES_BUFFER = SdkBytes.fromByteArray(new byte[] {});

    private static final Function<SdkBytes, String> SDK_BYTES_STRING = sdkBytes -> {
        if (sdkBytes == null || sdkBytes.equals(EMPTY_SDK_BYTES_BUFFER)) {
            return null;
        } else {
            return sdkBytes.asString(UTF_8);
        }

    };

    public static Message<String> kinesisMessage(final @Nonnull String shard,
                                                 final @Nonnull Record record) {
        return message(
                record.partitionKey(),
                responseHeader(
                        fromPosition(shard, record.sequenceNumber()),
                        record.approximateArrivalTimestamp()
                ),
                SDK_BYTES_STRING.apply(record.data()));
    }

}
