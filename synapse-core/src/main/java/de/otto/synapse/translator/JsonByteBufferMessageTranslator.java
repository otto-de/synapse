package de.otto.synapse.translator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.message.Message;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;

import static de.otto.synapse.message.Messages.byteBufferMessage;
import static de.otto.synapse.translator.ObjectMappers.defaultObjectMapper;
import static java.nio.ByteBuffer.wrap;

/**
 * A MessageTranslator that converts messages into messages with ByteBuffer JSON payloads.
 * <p>
 *     The Message Translator is the messaging equivalent of the Adapter pattern described in
 *     [GoF]. An adapter converts the interface of a component into a another interface so it
 *     can be used in a different context.
 * </p>
 * <p>
 *     <img src="http://www.enterpriseintegrationpatterns.com/img/MessageTranslator.gif" alt="MessageTranslator">
 * </p>
 */
public class JsonByteBufferMessageTranslator implements MessageTranslator<ByteBuffer> {

    private final ObjectMapper objectMapper;

    public JsonByteBufferMessageTranslator() {
        this.objectMapper = defaultObjectMapper();
    }

    public JsonByteBufferMessageTranslator(final @Nonnull ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     * Translates a Message into a Message with payload-type &lt;ByteBuffer&gt; and
     * serializes the payload into a JSON ByteBuffer.
     *
     * @param message Message&lt;?&gt;
     * @return Message&lt;ByteBuffer&gt;
     */
    @Override
    @Nonnull
    public Message<ByteBuffer> translate(final @Nonnull Message<?> message) {
        if (message.getPayload() == null) {
            return byteBufferMessage(message.getKey(), message.getHeader(), (ByteBuffer) null);
        } else {
            try {
                final byte[] payload = objectMapper.writeValueAsBytes(message.getPayload());
                return byteBufferMessage(message.getKey(), message.getHeader(), wrap(payload));
            } catch (JsonProcessingException e) {
                throw new IllegalStateException(e.getMessage());
            }
        }
    }
}
