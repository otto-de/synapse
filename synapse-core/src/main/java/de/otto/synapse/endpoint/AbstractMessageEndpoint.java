package de.otto.synapse.endpoint;

import de.otto.synapse.endpoint.receiver.MessageReceiverEndpoint;
import de.otto.synapse.endpoint.sender.MessageSenderEndpoint;
import de.otto.synapse.message.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static java.util.Objects.requireNonNull;

/**
 * Abstract {@code MessageEndpoint} that can be used to implement {@link MessageSenderEndpoint message sender} or
 * {@link MessageReceiverEndpoint message receiver} endpoints.
 */
public abstract class AbstractMessageEndpoint implements MessageEndpoint {

    private final String channelName;
    private final MessageInterceptor messageInterceptor;

    /**
     * Constructor used to create a new AbstractMessageEndpoint.
     *
     * @param channelName the name of the underlying channel / stream / queue / message log.
     */
    public AbstractMessageEndpoint(final @Nonnull String channelName) {
        this(channelName, (m) -> m);
    }

    /**
     * Constructor used to create a new AbstractMessageEndpoint.
     *
     * @param channelName the name of the underlying channel / stream / queue / message log.
     * @param messageInterceptor the MessageInterceptor used to intercept messages
     */
    public AbstractMessageEndpoint(final @Nonnull String channelName,
                                   final @Nonnull MessageInterceptor messageInterceptor) {
        this.channelName = requireNonNull(channelName, "ChannelName must not be null");
        this.messageInterceptor = requireNonNull(messageInterceptor, "MessageInterceptor must not be null");
    }

    /**
     * Returns the name of the channel.
     *
     * <p>
     *     The channel name corresponds to the name of the underlying stream, queue or message log.
     * </p>
     * @return name of the channel
     */
    @Override
    @Nonnull
    public final String getChannelName() {
        return channelName;
    }

    /**
     * Intercepts a message using all registered interceptors and returns the resulting message.
     * <p>
     *     The interceptors are called in order. The result of one interceptor is propagated to the
     *     next interceptor in the chain, until the end of the chain is reached, or one interceptor
     *     has returned null.
     * </p>
     * <p>
     *     If {@code null} is returned, the message must be dropped by the {@link MessageEndpoint}.
     * </p>
     * <p>
     *     Every interceptor may transform the message, or may take additional actions like, for example,
     *     logging, monitoring or other things.
     * </p>
     *
     * @param message the message to intercept
     * @return the (possibly modified) message, or null if the message should be dropped.
     */
    @Override
    @Nullable
    public Message<String> intercept(final Message<String> message) {
        return messageInterceptor.intercept(message);
    }

}
