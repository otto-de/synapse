package de.otto.synapse.endpoint.receiver;

import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.ShardResponse;
import de.otto.synapse.channel.StartFrom;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.consumer.MessageDispatcher;
import de.otto.synapse.endpoint.EndpointType;
import de.otto.synapse.endpoint.InterceptorChain;
import de.otto.synapse.message.TextMessage;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

public class DelegateMessageLogReceiverEndpoint implements MessageLogReceiverEndpoint {

    private final MessageLogReceiverEndpoint delegate;
    private final StartFrom iteratorAt;

    public DelegateMessageLogReceiverEndpoint(final @Nonnull String channelName,
                                              final @Nonnull String iteratorAt,
                                              final @Nonnull MessageLogReceiverEndpointFactory messageLogReceiverEndpointFactory) {
        this.delegate = messageLogReceiverEndpointFactory.create(channelName, StartFrom.valueOf(iteratorAt));
        this.iteratorAt = StartFrom.valueOf(iteratorAt);
    }

    @Nonnull
    @Override
    public CompletableFuture<ChannelPosition> consumeUntil(final @Nonnull ChannelPosition startFrom,
                                                           final @Nonnull Predicate<ShardResponse> stopCondition) {
        return delegate.consumeUntil(startFrom, stopCondition);
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
    public TextMessage intercept(final @Nonnull TextMessage message) {
        return delegate.intercept(message);
    }

    @Override
    public StartFrom getIterator() {
        return iteratorAt;
    }
}
