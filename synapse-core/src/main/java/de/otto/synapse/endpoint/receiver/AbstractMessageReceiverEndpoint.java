package de.otto.synapse.endpoint.receiver;

import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.endpoint.AbstractMessageEndpoint;
import de.otto.synapse.endpoint.MessageInterceptor;

import javax.annotation.Nonnull;

public class AbstractMessageReceiverEndpoint extends AbstractMessageEndpoint implements MessageReceiverEndpoint {

    public AbstractMessageReceiverEndpoint(@Nonnull String channelName) {
        super(channelName);
    }

    public AbstractMessageReceiverEndpoint(@Nonnull String channelName, @Nonnull MessageInterceptor messageInterceptor) {
        super(channelName, messageInterceptor);
    }

    @Override
    public <T> void register(MessageConsumer<T> messageConsumer, Class<T> payloadType) {

    }

    @Override
    public MessageConsumer messageConsumer() {
        return null;
    }
}
