package de.otto.synapse.endpoint.receiver;

import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.ChannelResponse;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.message.Message;

import javax.annotation.Nonnull;
import java.util.function.Predicate;

public interface MessageLogReceiverEndpoint extends MessageReceiverEndpoint {

    @Nonnull
    String getChannelName();

    @Nonnull
    ChannelPosition consume(@Nonnull ChannelPosition startFrom,
                            @Nonnull Predicate<Message<?>> stopCondition,
                            @Nonnull MessageConsumer<String> consumer);

    public void stop();
}
