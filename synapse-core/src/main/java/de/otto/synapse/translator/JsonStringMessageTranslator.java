package de.otto.synapse.translator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.message.Message;

import javax.annotation.Nonnull;

import static de.otto.synapse.message.Messages.stringMessage;
import static de.otto.synapse.translator.ObjectMappers.currentObjectMapper;

/**
 * A MessageTranslator that converts messages into messages with String JSON payloads.
 * <p>
 *     The Message Translator is the messaging equivalent of the Adapter pattern described in
 *     [GoF]. An adapter converts the interface of a component into a another interface so it
 *     can be used in a different context.
 * </p>
 * <p>
 *     <img src="http://www.enterpriseintegrationpatterns.com/img/MessageTranslator.gif" alt="MessageTranslator">
 * </p>
 */
public class JsonStringMessageTranslator implements MessageTranslator<String> {

    private final ObjectMapper objectMapper;

    public JsonStringMessageTranslator() {
        this.objectMapper = currentObjectMapper();
    }

    public JsonStringMessageTranslator(final @Nonnull ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

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
    public Message<String> translate(final @Nonnull Message<?> message) {
        try {
            if (message.getPayload() instanceof String) {
                return (Message<String>) message;
            } else {
                final String payload = message.getPayload() != null
                        ? objectMapper.writeValueAsString(message.getPayload())
                        : null;
                return stringMessage(message.getKey(), message.getHeader(), payload);
            }
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}
