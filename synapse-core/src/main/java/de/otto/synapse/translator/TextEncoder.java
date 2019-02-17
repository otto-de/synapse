package de.otto.synapse.translator;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import de.otto.synapse.message.Message;

import java.io.IOException;

import static de.otto.synapse.translator.MessageFormat.*;
import static de.otto.synapse.translator.ObjectMappers.currentObjectMapper;

public class TextEncoder implements Encoder<String> {

    private final MessageFormat messageFormat;

    public TextEncoder() {
        this(defaultMessageFormat());
    }

    public TextEncoder(final MessageFormat messageFormat) {
        this.messageFormat = messageFormat;
    }

    @Override
    public String apply(final Message<String> message) {
        return encode(message, messageFormat);
    }


    protected String encode(final Message<String> message,
                            final MessageFormat messageFormat) {
        switch (messageFormat) {
            case V1:
                return message.getPayload();
            case V2:
                return encodeV2(message);
            default:
                throw new IllegalStateException("Unsupported MessageFormat " + messageFormat);
        }
    }

    private String encode(final Message<String> message) {
        return encode(message, defaultMessageFormat());
    }

    private static String encodeV2(Message<String> message) {
        ObjectMapper mapper = currentObjectMapper();
        try {
            ObjectNode root = mapper.createObjectNode();
            root.put(SYNAPSE_MSG_FORMAT, "v2");
            root.set(SYNAPSE_MSG_KEY, encodeKeysV2(message, mapper));
            root.set(SYNAPSE_MSG_HEADERS, encodeHeadersV2(message, mapper));
            root.set(SYNAPSE_MSG_PAYLOAD, encodePayloadV2(message, mapper));
            return root.toString();
        } catch (IOException e) {
            throw new IllegalStateException(String.format("Cannot encode message %s", message), e);
        }
    }

    private static ObjectNode encodeKeysV2(Message<String> message, ObjectMapper mapper) {
        ObjectNode keyNode = mapper.createObjectNode();
        keyNode.put(SYNAPSE_MSG_PARTITIONKEY, message.getKey().partitionKey());
        keyNode.put(SYNAPSE_MSG_COMPACTIONKEY, message.getKey().compactionKey());
        return keyNode;
    }

    private static JsonNode encodeHeadersV2(Message<String> message, ObjectMapper mapper) {
        return mapper.convertValue(message.getHeader().getAll(), JsonNode.class);
    }

    private static JsonNode encodePayloadV2(Message<String> message, ObjectMapper mapper) throws IOException {
        JsonNode jsonPayload;
        try {
            if (message.getPayload() == null) {
                jsonPayload = NullNode.getInstance();
            } else {
                jsonPayload = mapper.readTree(message.getPayload());
            }
        } catch (JsonParseException e) {
            jsonPayload = new TextNode(message.getPayload());
        }
        return jsonPayload;
    }
}
