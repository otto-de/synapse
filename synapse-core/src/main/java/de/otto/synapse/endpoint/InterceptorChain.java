package de.otto.synapse.endpoint;

import com.google.common.collect.ImmutableList;
import de.otto.synapse.endpoint.sender.AbstractMessageSenderEndpoint;
import de.otto.synapse.message.Message;
import org.slf4j.Logger;
import org.springframework.core.OrderComparator;
import org.springframework.core.annotation.AnnotationAwareOrderComparator;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.Comparator;

import static com.google.common.collect.ImmutableList.sortedCopyOf;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Implementation of the Composite pattern for {@link MessageInterceptor message interceptors} that
 * can be used to build chains of interceptors.
 * <p>
 *     Just like any other {@link MessageInterceptor}, the InterceptorChain is used to process
 *     a message before it is consumed by {@link de.otto.synapse.consumer.MessageConsumer message consumers}
 *     on the receiver-side, or before it is sent by a {@link AbstractMessageSenderEndpoint message sender}
 *     to the channel infrastructure.
 * </p>
 * <p>
 *     The interceptors of the InterceptorChain will be called in order. The result of the first
 *     interceptor is propagated to the next interceptor, and so on.
 * </p>
 * <p>
 *     If an interceptor returns null, the chain will return null without further processing of the message.
 * </p>
 *
 * @see <a href="https://en.wikipedia.org/wiki/Composite_pattern">Composite Pattern</a>
 */
@ThreadSafe
public final class InterceptorChain implements MessageInterceptor {

    private static final Logger LOG = getLogger(InterceptorChain.class);

    private final ImmutableList<MessageInterceptor> interceptors;

    /**
     * Creates an empty InterceptorChain.
     */
    public InterceptorChain() {
        this.interceptors = ImmutableList.of();
    }

    public InterceptorChain(final ImmutableList<MessageInterceptor> messageInterceptors) {
        this.interceptors = messageInterceptors;
    }

    /**
     * Returns the immutable list of registered {@link MessageInterceptor message interceptors}.
     *
     * @return registered message interceptors
     */
    public ImmutableList<MessageInterceptor> getInterceptors() {
        return interceptors;
    }

    /**
     * Intercepts a message using all registered interceptors and returns the resulting message.
     * <p>
     *     The interceptors are called in order. The result of one interceptor is propagated to the
     *     next interceptor in the chain, until the end of the chain is reached, or one interceptor
     *     has returned null.
     * </p>
     * <p>
     *     If {@code null} is returned, the message must be dropped by the {@link AbstractMessageEndpoint}.
     * </p>
     * <p>
     *     Every interceptor may transform the message, or may take additional actions like, for example,
     *     logging, monitoring or other things.
     * </p>
     *
     * @param message the message to intercept
     * @return the (possibly modified) message, or null if the message should be dropped.
     */
    @Nullable
    public Message<String> intercept(final @Nonnull Message<String> message) {
        Message<String> interceptedMessage = message;
        for (final MessageInterceptor interceptor : interceptors) {
            if (interceptedMessage == null) {
                break;
            }
            interceptedMessage = interceptor.intercept(interceptedMessage);
        }
        if (interceptedMessage != null) {
            LOG.debug("Intercepted message '{}' converted to {}", message, interceptedMessage);
        } else {
            LOG.debug("Intercepted message '{}' converted to <null> - dropping message", message);
        }
        return interceptedMessage;
    }

}
