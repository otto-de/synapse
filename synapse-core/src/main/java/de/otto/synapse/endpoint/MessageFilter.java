package de.otto.synapse.endpoint;

import de.otto.synapse.message.TextMessage;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;

import java.util.function.Predicate;

/**
 * A {@link MessageInterceptor} that is used to filter messages using some predicate.
 *
 * <p>
 *     MessageFilter will usually be chained using a {@link InterceptorChain}.
 * </p>
 * <p>
 *     <img src="http://www.enterpriseintegrationpatterns.com/img/MessageFilter.gif" alt="Message Filter">
 * </p>
 *
 * @see <a href="http://www.enterpriseintegrationpatterns.com/patterns/messaging/Filter.html">EIP: Message Filter</a>
 */
public class MessageFilter implements MessageInterceptor {

    private final Predicate<TextMessage> predicate;

    private MessageFilter(final Predicate<TextMessage> predicate) {
        this.predicate = predicate;
    }

    /**
     * Creates a {@link MessageFilter} that is used to filter messages from {@link AbstractMessageEndpoint message endpoints}.
     * <p>
     *     Messages will be dropped, if {@code predicate.test(message)} is {@code false}
     * </p>
     *
     * @param predicate the predicate used to select messages
     * @return MessageFilter
     */
    public static MessageFilter messageFilter(final Predicate<TextMessage> predicate) {
        return new MessageFilter(predicate);
    }

    /**
     * {@inheritDoc}
     *
     * The {@code intercept} method of a MessageFilter returns the intercepted message, if the filter predicate returns
     * {@code true}. Otherwise, {@code null} is returned and the message will be dropped.
     *
     * @param message the channel-layer message with payload-type beeing a String
     * @return intercepted version of the message, or null if the message should be dropped.
     */
    @Nullable
    public final TextMessage intercept(final @Nonnull TextMessage message) {
        return predicate.test(message) ? message : null;
    }
}
