package de.otto.synapse.endpoint.sender;

import de.otto.synapse.endpoint.EndpointType;
import de.otto.synapse.endpoint.InterceptorChain;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.message.Message;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.stream.Stream;

import static org.slf4j.LoggerFactory.getLogger;

public class DelegateMessageQueueSenderEndpoint implements MessageSenderEndpoint{

    private static final Logger LOG = getLogger(DelegateMessageQueueSenderEndpoint.class);
    private final MessageSenderEndpoint delegate;

    public DelegateMessageQueueSenderEndpoint(final @Nonnull String channelName,
                                              final @Nonnull MessageSenderEndpointFactory messageQueueSenderEndpointFactory) {
        this.delegate = messageQueueSenderEndpointFactory.create(channelName);
        LOG.info("Created MessageQueueReceiverEndpoint for channelName={}", channelName);
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

    @Override
    public <T> void send(@Nonnull Message<T> message) {
        delegate.send(message);
    }

    @Override
    public <T> void sendBatch(@Nonnull Stream<Message<T>> batch) {
        delegate.sendBatch(batch);
    }
}
