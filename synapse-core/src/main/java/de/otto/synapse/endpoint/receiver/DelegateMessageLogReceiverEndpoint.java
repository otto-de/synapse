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

public class DelegateMessageLogReceiverEndpoint implements MessageLogReceiverEndpoint {

    private final MessageLogReceiverEndpoint delegate;

    public DelegateMessageLogReceiverEndpoint(final @Nonnull String channelName,
                                              final @Nonnull MessageLogReceiverEndpointFactory messageLogReceiverEndpointFactory) {
        this.delegate = messageLogReceiverEndpointFactory.create(channelName);
    }

    @Nonnull
    @Override
    public CompletableFuture<ChannelPosition> consumeUntil(final @Nonnull ChannelPosition startFrom,
                                                           final @Nonnull Instant until) {
        return delegate.consumeUntil(startFrom, until);
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
