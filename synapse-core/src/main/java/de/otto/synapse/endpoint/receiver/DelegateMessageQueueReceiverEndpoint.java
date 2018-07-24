package de.otto.synapse.endpoint.receiver;

import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.consumer.MessageDispatcher;
import de.otto.synapse.endpoint.EndpointType;
import de.otto.synapse.endpoint.InterceptorChain;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.message.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;

public class DelegateMessageQueueReceiverEndpoint implements MessageQueueReceiverEndpoint {

    private final MessageQueueReceiverEndpoint delegate;

    public DelegateMessageQueueReceiverEndpoint(final @Nonnull String channelName,
                                                final @Nonnull MessageQueueReceiverEndpointFactory messageQueueReceiverEndpointFactory) {
        this.delegate = messageQueueReceiverEndpointFactory.create(channelName);
    }


    @Override
    public CompletableFuture<Void> consume() {
        return delegate.consume();
    }

    @Override
    public void stop() {
        delegate.stop();
    }

    @Override
    public void register(final MessageConsumer<?> messageConsumer) {
        delegate.register(messageConsumer);
    }

    @Nonnull
    @Override
    public MessageDispatcher getMessageDispatcher() {
        return delegate.getMessageDispatcher();
    }

    @Nonnull
    @Override
    public String getChannelName() {
        return delegate.getChannelName();
    }

    @Nonnull
    @Override
    public InterceptorChain getInterceptorChain() {
        return delegate.getInterceptorChain();
    }

    @Override
    public void registerInterceptorsFrom(final @Nonnull MessageInterceptorRegistry registry) {
        delegate.registerInterceptorsFrom(registry);
    }

    @Nonnull
    @Override
    public EndpointType getEndpointType() {
        return delegate.getEndpointType();
    }

    @Nullable
    @Override
    public Message<String> intercept(final @Nonnull Message<String> message) {
        return delegate.intercept(message);
    }
}
