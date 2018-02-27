package de.otto.synapse.aws.kinesis;

import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.consumer.StreamPosition;
import de.otto.synapse.message.Message;

import java.util.function.Predicate;

public interface MessageLog {

    String getStreamName();

    StreamResponse consumeStream(StreamPosition startFrom,
                                 Predicate<Message<?>> stopCondition,
                                 MessageConsumer<String> consumer);
}
