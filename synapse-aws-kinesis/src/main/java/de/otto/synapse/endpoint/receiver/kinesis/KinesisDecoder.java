package de.otto.synapse.endpoint.receiver.kinesis;

import de.otto.synapse.message.Header;
import de.otto.synapse.message.Key;
import de.otto.synapse.message.TextMessage;
import de.otto.synapse.translator.AbstractTextDecoder;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.model.Record;

import java.util.function.Function;

import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static de.otto.synapse.message.DefaultHeaderAttr.MSG_ARRIVAL_TS;
import static java.nio.charset.StandardCharsets.UTF_8;
import static software.amazon.awssdk.core.SdkBytes.fromByteArray;

public class KinesisDecoder extends AbstractTextDecoder<RecordWithShard> {

    private static final SdkBytes EMPTY_SDK_BYTES_BUFFER = fromByteArray(new byte[]{});

    private static final Function<SdkBytes, String> SDK_BYTES_STRING = sdkBytes -> {
        if (sdkBytes == null || sdkBytes.equals(EMPTY_SDK_BYTES_BUFFER)) {
            return null;
        } else {
            return sdkBytes.asString(UTF_8);
        }
    };

    @Override
    public TextMessage apply(RecordWithShard recordWithShard) {
        final Record record = recordWithShard.getRecord();
        final String shardName = recordWithShard.getShardName();
        return decode(
                Key.of(record.partitionKey()),
                Header.builder()
                        .withAttribute(MSG_ARRIVAL_TS, record.approximateArrivalTimestamp())
                        .withShardPosition(fromPosition(shardName, record.sequenceNumber())).build(),
                SDK_BYTES_STRING.apply(record.data()));
    }


}
