package de.otto.synapse.endpoint;

import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.ChannelResponse;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.message.Message;

import java.util.function.Predicate;

public interface MessageLogReceiverEndpoint {

    String getChannelName();

    ChannelPosition consume(ChannelPosition startFrom,
                            Predicate<Message<?>> stopCondition,
                            MessageConsumer<String> consumer);

    public void stop();
}
