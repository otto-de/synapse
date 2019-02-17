package de.otto.synapse.translator;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import de.otto.synapse.message.Header;
import de.otto.synapse.message.Key;
import de.otto.synapse.message.Message;
import de.otto.synapse.message.TextMessage;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import static de.otto.synapse.message.Header.copyOf;
import static de.otto.synapse.translator.MessageFormat.defaultMessageFormat;
import static de.otto.synapse.translator.ObjectMappers.currentObjectMapper;
import static java.util.Collections.emptyMap;
import static org.slf4j.LoggerFactory.getLogger;

public class MessageCodec {

    // TODO: MessageCodec als Bean, damit man den Codec bei Bedarf austauschen / anpassen kann. Eine weitere Bean MessageCodecs könnte sich um das Format kümmern.

    public static final String SYNAPSE_MSG_FORMAT = "_synapse_msg_format";
    public static final String SYNAPSE_MSG_KEY = "_synapse_msg_key";
    public static final String SYNAPSE_MSG_COMPACTIONKEY = "compactionKey";
    public static final String SYNAPSE_MSG_PARTITIONKEY = "partitionKey";
    public static final String SYNAPSE_MSG_HEADERS = "_synapse_msg_headers";
    public static final String SYNAPSE_MSG_PAYLOAD = "_synapse_msg_payload";

    public static final Pattern V2_PATTERN = Pattern.compile("\\{\\s*\"" + SYNAPSE_MSG_FORMAT + "\"\\s*:\\s*\"v2\".+");


    private static final Logger LOG = getLogger(MessageCodec.class);
    private static final TypeReference<Map<String, String>> MAP_TYPE_REFERENCE = new TypeReference<Map<String, String>>() {
    };

    public static String encode(final Message<String> message) {
        return encode(message, defaultMessageFormat());
    }

    public static String encode(final Message<String> message,
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

    public static TextMessage decode(final String body) {
        return decode(Key.of(), Header.of(), body);
    }

    public static TextMessage decode(final Key key,
                                     final Header prototypeHeader,
                                     final String body) {
        switch (versionOf(body)) {
            case V1:
                return TextMessage.of(key, prototypeHeader, body);
            case V2:
                try {
                    final JsonNode json = parseRecordBody(body);
                    return TextMessage.of(
                            keyFrom(json).orElse(key),
                            copyOf(prototypeHeader)
                                    .withAttributes(attributesFrom(json))
                                    .build(),
                            payloadFrom(json));
                } catch (final RuntimeException e) {
                    LOG.error("Exception caught while parsing record {}: {}", body, e.getMessage());
                    return TextMessage.of(key, prototypeHeader, body);
                }
            default:
                throw new IllegalStateException("Unsupported message format: " + body);
        }
    }

    private static MessageFormat versionOf(final String body) {
        if (body != null) {
            return V2_PATTERN.matcher(body).matches()
                    ? MessageFormat.V2
                    : MessageFormat.V1;
        } else {
            return MessageFormat.V1;
        }
    }

    private static Map<String, String> attributesFrom(final JsonNode json) {
        final JsonNode headersJson = json.get(SYNAPSE_MSG_HEADERS);
        if (headersJson != null) {
            return currentObjectMapper().convertValue(
                    headersJson,
                    MAP_TYPE_REFERENCE
            );
        } else {
            return emptyMap();
        }
    }

    private static Optional<Key> keyFrom(final JsonNode json) {
        final JsonNode keyNode = json.get(SYNAPSE_MSG_KEY);
        if (keyNode == null || keyNode.isNull()) {
            return Optional.empty();
        } else if (keyNode.isObject()) {
            final String partitionKey = keyNode.get(SYNAPSE_MSG_PARTITIONKEY).textValue();
            final String compactionKey = keyNode.get(SYNAPSE_MSG_COMPACTIONKEY).textValue();
            return Optional.of(Key.of(partitionKey, compactionKey));
        } else {
            final String msg = "Unexpected json node containing " + json + ": ";
            LOG.error(msg);
            throw new IllegalStateException(msg);
        }
    }

    private static String payloadFrom(final JsonNode json) {
        final JsonNode payloadJson = json.get(SYNAPSE_MSG_PAYLOAD);
        if (payloadJson == null || payloadJson.isNull()) {
            return null;
        } else if (payloadJson.isObject()) {
            return payloadJson.toString();
        } else {
            return payloadJson.asText();
        }
    }

    private static JsonNode parseRecordBody(String body) {
        try {
            return currentObjectMapper().readTree(body);
        } catch (IOException e) {
            LOG.error("Error parsing body={} from Kinesis record: {}", body, e.getMessage());
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}
