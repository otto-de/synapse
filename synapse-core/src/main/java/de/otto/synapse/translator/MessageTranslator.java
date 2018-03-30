package de.otto.synapse.translator;

import de.otto.synapse.message.Message;

import javax.annotation.Nonnull;
import java.util.function.Function;

import static de.otto.synapse.message.Message.message;

/**
 * <p>
 *     The Message Translator is the messaging equivalent of the Adapter pattern described in
 *     [GoF]. An adapter converts the interface of a component into a another interface so it
 *     can be used in a different context.
 * </p>
 * <p>
 *     <img src="http://www.enterpriseintegrationpatterns.com/img/MessageTranslator.gif" alt="MessageTranslator">
 * </p>
 * @param <P> The type of the translated message's payload
 * @see <a href="http://www.enterpriseintegrationpatterns.com/patterns/messaging/MessageTranslator.html">EIP: Message Translator</a>
 */
@FunctionalInterface
public interface MessageTranslator<P> {

    /**
     * Creates a MessageTranslator that is translating the message payload using the specified {@link Function},
     * +while keeping {@link Message#getKey()} and {@link Message#getHeader()} as-is.
     *
     * @param payloadTranslator the function used to translate the {@link Message#getPayload() message payload}.
     * @param <P> The type of the resulting Message's payload.
     * @return MessageTranslator
     */
    static <P> MessageTranslator<P> of(Function<Object,P> payloadTranslator) {
        return new MessageTranslator<P>() {
            @Nonnull
            @Override
            public Message<P> translate(@Nonnull Message<?> message) {
                Object payload = message.getPayload();
                return message(message.getKey(), message.getHeader(), payloadTranslator.apply(payload));
            }
        };
    }

    /**
     * Translates a Message into a Message with payload-type &lt;P&gt;
     *
     * @param message Message&lt;?&gt;
     * @return Message&lt;P&gt;
     */
    @Nonnull Message<P> translate(final @Nonnull Message<?> message);

}
