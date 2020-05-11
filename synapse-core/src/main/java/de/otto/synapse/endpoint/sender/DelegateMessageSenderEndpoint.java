package de.otto.synapse.endpoint.sender;

import de.otto.synapse.channel.selector.Selector;
import de.otto.synapse.endpoint.BestMatchingSelectableComparator;
import de.otto.synapse.endpoint.EndpointType;
import de.otto.synapse.endpoint.InterceptorChain;
import de.otto.synapse.message.Message;
import de.otto.synapse.message.TextMessage;
import de.otto.synapse.translator.MessageFormat;
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
    @Nonnull
    private final MessageFormat messageFormat;

    public DelegateMessageSenderEndpoint(final @Nonnull String channelName,
                                         final @Nonnull Class<? extends Selector> selector,
                                         final @Nonnull MessageFormat messageFormat,
                                         final List<MessageSenderEndpointFactory> factories) {
        this.messageFormat = messageFormat;
        final MessageSenderEndpointFactory messageSenderEndpointFactory = factories
                .stream()
                .filter(factory -> factory.matches(selector))
                .min(new BestMatchingSelectableComparator(selector))
                .orElseThrow(() -> new IllegalStateException(format("Unable to create MessageSenderEndpoint for channelName=%s: no matching MessageSenderEndpointFactory found in the ApplicationContext.", channelName)));
        this.delegate = messageSenderEndpointFactory.create(channelName, messageFormat);
        LOG.info("Created MessageSenderEndpoint for channelName={}", channelName);
    }

    @Nonnull
    @Override
    public String getChannelName() {
        return delegate.getChannelName();
    }

    @Nonnull
    public MessageFormat getMessageFormat() {
        return messageFormat;
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
    public CompletableFuture<Void> send(@Nonnull Message<?> message) {
        return delegate.send(message);
    }

    @Override
    public CompletableFuture<Void> sendBatch(@Nonnull Stream<? extends Message<?>> batch) {
        return delegate.sendBatch(batch);
    }
}
