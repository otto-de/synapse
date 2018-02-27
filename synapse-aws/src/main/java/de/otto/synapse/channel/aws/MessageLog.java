package de.otto.synapse.channel.aws;

import de.otto.synapse.channel.StreamPosition;
import de.otto.synapse.channel.StreamResponse;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.message.Message;

import java.util.function.Predicate;

public interface MessageLog {

    String getStreamName();

    StreamResponse consumeStream(StreamPosition startFrom,
                                 Predicate<Message<?>> stopCondition,
                                 MessageConsumer<String> consumer);
}
