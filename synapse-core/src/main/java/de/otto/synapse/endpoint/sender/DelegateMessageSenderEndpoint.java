package de.otto.synapse.endpoint.sender;

import de.otto.synapse.channel.selector.Selector;
import de.otto.synapse.endpoint.EndpointType;
import de.otto.synapse.endpoint.InterceptorChain;
import de.otto.synapse.message.Message;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static java.lang.String.format;
import static org.slf4j.LoggerFactory.getLogger;

public class DelegateMessageSenderEndpoint implements MessageSenderEndpoint{

    private static final Logger LOG = getLogger(DelegateMessageSenderEndpoint.class);
    private final MessageSenderEndpoint delegate;

    public DelegateMessageSenderEndpoint(final @Nonnull String channelName,
                                         final @Nullable Class<? extends Selector> selector,
                                         final List<MessageSenderEndpointFactory> factories) {
        final MessageSenderEndpointFactory messageSenderEndpointFactory = factories
                .stream()
                .filter(factory -> factory.matches(selector))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(format("Unable to create MessageSenderEndpoint for channelName=%s: no matching MessageSenderEndpointFactory found in the ApplicationContext.", channelName)));
        this.delegate = messageSenderEndpointFactory.create(channelName);
        LOG.info("Created MessageSenderEndpoint for channelName={}", channelName);
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

    @Override
    public <T> CompletableFuture<Void> send(@Nonnull Message<T> message) {
        return delegate.send(message);
    }

    @Override
    public <T> CompletableFuture<Void> sendBatch(@Nonnull Stream<Message<T>> batch) {
        return delegate.sendBatch(batch);
    }
}
