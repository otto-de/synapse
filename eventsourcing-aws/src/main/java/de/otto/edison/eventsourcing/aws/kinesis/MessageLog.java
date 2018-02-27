package de.otto.edison.eventsourcing.aws.kinesis;

import de.otto.edison.eventsourcing.consumer.MessageConsumer;
import de.otto.edison.eventsourcing.consumer.StreamPosition;
import de.otto.edison.eventsourcing.message.Message;

import java.util.function.Predicate;

public interface MessageLog {

    String getStreamName();

    StreamResponse consumeStream(StreamPosition startFrom,
                                 Predicate<Message<?>> stopCondition,
                                 MessageConsumer<String> consumer);
}
