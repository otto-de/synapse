package de.otto.synapse.endpoint.receiver;

import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.consumer.MessageDispatcher;
import de.otto.synapse.endpoint.EndpointType;
import de.otto.synapse.endpoint.InterceptorChain;
import de.otto.synapse.message.TextMessage;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.slf4j.Logger;

import java.util.concurrent.CompletableFuture;

import static org.slf4j.LoggerFactory.getLogger;

public class DelegateMessageQueueReceiverEndpoint implements MessageQueueReceiverEndpoint {

    private static final Logger LOG = getLogger(DelegateMessageQueueReceiverEndpoint.class);
    private final MessageQueueReceiverEndpoint delegate;

    public DelegateMessageQueueReceiverEndpoint(final @Nonnull String channelName,
                                                final @Nonnull MessageQueueReceiverEndpointFactory messageQueueReceiverEndpointFactory) {
        this.delegate = messageQueueReceiverEndpointFactory.create(channelName);
        LOG.info("Created MessageQueueReceiverEndpoint for channelName={}", channelName);
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
}
