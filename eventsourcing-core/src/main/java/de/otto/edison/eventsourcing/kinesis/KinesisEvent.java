package de.otto.edison.eventsourcing.kinesis;

import de.otto.edison.eventsourcing.consumer.Event;
import software.amazon.awssdk.services.kinesis.model.Record;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.function.Function;

public class KinesisEvent<T> extends Event<T> {

    public static <T> Event<T> kinesisEvent(final Record record,
                                            final Function<ByteBuffer, T> decoder) {
        return new KinesisEvent<>(
                record,
                null,
                decoder);
    }

    public static <T> Event<T> kinesisEvent(final Duration durationBehind,
                                            final Record record,
                                            final Function<ByteBuffer, T> decoder) {
        return new KinesisEvent<>(
                record,
                durationBehind,
                decoder);
    }

    private KinesisEvent(final Record record,
                         final Duration durationBehind,
                         final Function<ByteBuffer, T> decoder) {
        super(
                record.partitionKey(),
                decoder.apply(record.data()),
                record.sequenceNumber(),
                record.approximateArrivalTimestamp(),
                durationBehind);
    }

}
