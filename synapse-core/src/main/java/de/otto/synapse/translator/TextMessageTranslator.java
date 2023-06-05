package de.otto.synapse.translator;

import com.fasterxml.jackson.core.JsonProcessingException;
import de.otto.synapse.message.Message;
import de.otto.synapse.message.TextMessage;
import jakarta.annotation.Nonnull;

import static de.otto.synapse.translator.ObjectMappers.currentObjectMapper;

/**
 * A MessageTranslator that converts messages into {@link TextMessage}.
 *
 * <p>The Message Translator is the messaging equivalent of the Adapter pattern described in
 *     [GoF]. An adapter converts the interface of a component into a another interface so it
 *     can be used in a different context.</p>
 *
 * <p><img src="http://www.enterpriseintegrationpatterns.com/img/MessageTranslator.gif" alt="MessageTranslator"></p>
 *
 * <p>This implementation is relying on {@link ObjectMappers#currentObjectMapper()} to transform message payloads
 * with non-String payload into a JSON payload.</p>
 */
public class TextMessageTranslator implements MessageTranslator<TextMessage> {

    /**
     * Translates a Message into a Message with payload-type &lt;String&gt; and
     * serializes the payload into a JSON String.
     *
     * @param message Message&lt;?&gt;
     * @return Message&lt;String&gt;
     */
    @Override
    @Nonnull
    @SuppressWarnings("unchecked")
    public TextMessage apply(final @Nonnull Message<?> message) {
        try {
            if (message.getPayload() instanceof String) {
                return TextMessage.of((Message<String>)message);
            } else {
                final String payload = message.getPayload() != null
                        ? currentObjectMapper().writeValueAsString(message.getPayload())
                        : null;
                return TextMessage.of(message.getKey(), message.getHeader(), payload);
            }
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}
