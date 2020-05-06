package de.otto.synapse.endpoint.receiver;

import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.ShardResponse;
import de.otto.synapse.channel.selector.Selector;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.consumer.MessageDispatcher;
import de.otto.synapse.endpoint.BestMatchingSelectableComparator;
import de.otto.synapse.endpoint.EndpointType;
import de.otto.synapse.endpoint.InterceptorChain;
import de.otto.synapse.message.TextMessage;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

import static java.lang.String.format;

public class DelegateMessageLogReceiverEndpoint implements MessageLogReceiverEndpoint {

    private final MessageLogReceiverEndpoint delegate;

    public DelegateMessageLogReceiverEndpoint(final @Nonnull String channelName,
                                              final @Nonnull Class<? extends Selector> selector,
                                              final @Nonnull List<MessageLogReceiverEndpointFactory> factories) {
        final MessageLogReceiverEndpointFactory selectedEndpointFactory = factories
                .stream()
                .filter(factory -> factory.matches(selector))
                .min(new BestMatchingSelectableComparator(selector))
                .orElseThrow(() -> new IllegalStateException(format("Unable to create MessageLogReceiverEndpoint for channelName=%s: no matching MessageLogReceiverEndpointFactory found in the ApplicationContext.", channelName)));
        this.delegate = selectedEndpointFactory.create(channelName);
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

    public MessageLogReceiverEndpoint getDelegate() {
        return delegate;
    }
}
