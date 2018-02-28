package de.otto.synapse.channel.aws;

import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.ChannelResponse;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.message.Message;

import java.util.function.Predicate;

public interface MessageLog {

    String getStreamName();

    ChannelResponse consumeStream(ChannelPosition startFrom,
                                  Predicate<Message<?>> stopCondition,
                                  MessageConsumer<String> consumer);
}
