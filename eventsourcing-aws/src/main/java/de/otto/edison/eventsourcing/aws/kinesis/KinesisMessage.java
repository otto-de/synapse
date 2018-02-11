package de.otto.edison.eventsourcing.aws.kinesis;

import de.otto.edison.eventsourcing.message.Message;
import software.amazon.awssdk.services.kinesis.model.Record;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.function.Function;

import static de.otto.edison.eventsourcing.message.Header.responseHeader;

public class KinesisMessage<T> extends Message<T> {

    // TODO: wozu eine Decoder Function?

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
