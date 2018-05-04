package de.otto.synapse.endpoint;

import de.otto.synapse.endpoint.sender.MessageSenderEndpoint;
import de.otto.synapse.message.Message;
import org.slf4j.Logger;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Implementation of the Composite pattern for {@link MessageInterceptor message interceptors} that
 * can be used to build chains of interceptors.
 * <p>
 *     Just like any other {@link MessageInterceptor}, the InterceptorChain is used to process
 *     a message before it is consumed by {@link de.otto.synapse.consumer.MessageConsumer message consumers}
 *     on the receiver-side, or before it is sent by a {@link MessageSenderEndpoint message sender}
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

    private final List<MessageInterceptor> interceptors;

    /**
     * Creates an empty InterceptorChain.
     */
    public InterceptorChain() {
        this.interceptors = new CopyOnWriteArrayList<>();
    }

    public void register(final MessageInterceptor messageInterceptor) {
        // TODO: support ordering of interceptors via (org.springframework.core.annotation.Order)
        interceptors.add(messageInterceptor);
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
    @Nullable
    public Message<String> intercept(final Message<String> message) {
        LOG.debug("Intercepting message '{}' using {} interceptors", message, interceptors.size());
        Message<String> m = message;
        for (final MessageInterceptor interceptor : interceptors) {
            if (m != null) {
                m = interceptor.intercept(m);
            } else {
                LOG.debug("Interceptor returned <null> - dropping message");
            }
        }
        return m;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InterceptorChain chain = (InterceptorChain) o;
        return Objects.equals(interceptors, chain.interceptors);
    }

    @Override
    public int hashCode() {
        return Objects.hash(interceptors);
    }
}