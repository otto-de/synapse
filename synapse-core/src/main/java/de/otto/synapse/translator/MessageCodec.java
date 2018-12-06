package de.otto.synapse.translator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import de.otto.synapse.message.Header;
import de.otto.synapse.message.Message;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Pattern;

import static de.otto.synapse.translator.MessageFormat.defaultMessageFormat;
import static de.otto.synapse.translator.ObjectMappers.defaultObjectMapper;
import static java.util.Collections.emptyMap;
import static org.slf4j.LoggerFactory.getLogger;

public class MessageCodec {

    public static final String SYNAPSE_MSG_FORMAT = "_synapse_msg_format";
    public static final String SYNAPSE_MSG_HEADERS = "_synapse_msg_headers";
    public static final String SYNAPSE_MSG_PAYLOAD = "_synapse_msg_payload";

    public static final Pattern V2_PATTERN = Pattern.compile("\\{\\s*\"" + SYNAPSE_MSG_FORMAT + "\"\\s*:\\s*\"v2\".+");


    private static final Logger LOG = getLogger(MessageCodec.class);
    private static final TypeReference<Map<String, String>> MAP_TYPE_REFERENCE = new TypeReference<Map<String, String>>() {};

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
        final String jsonPayload;
        if (message.getPayload() == null) {
            jsonPayload = null;
        } else if (message.getPayload().isEmpty()) {
            // TODO: Das kann nur passieren, wenn man an der üblichen Serialisierung vorbei geht.
            jsonPayload = "\"\"";
        } else {
            final String trimedPayload = message.getPayload().trim();
            if ((trimedPayload.startsWith("{") && trimedPayload.endsWith("}")) || (trimedPayload.startsWith("[") && trimedPayload.endsWith("]"))) {
                jsonPayload = trimedPayload;
            } else {
                // TODO: Das kann nur passieren, wenn man an der üblichen Serialisierung vorbei geht.
                try {
                    jsonPayload = defaultObjectMapper().writeValueAsString(message.getPayload());
                } catch (JsonProcessingException e) {
                    throw new IllegalArgumentException("Unable to generate message payload from " + message.getPayload() + ": " + e.getMessage(), e);
                }
            }
        }
        String jsonHeaders;
        try {
            jsonHeaders = defaultObjectMapper().writeValueAsString(message.getHeader().getAttributes());
        } catch (final JsonProcessingException e) {
            LOG.error("Failed to convert message headers={} into JSON message format v2: {}", message.getHeader(), e.getMessage());
            jsonHeaders = "{}";
        }
        return "{\"_synapse_msg_format\":\"v2\","
                + "\"_synapse_msg_headers\":" + jsonHeaders + ","
                + "\"_synapse_msg_payload\":" + jsonPayload + "}";
    }

    public static Message<String> decode(final String body,
                                         final Header.Builder headerBuilder,
                                         final Message.Builder<String> messageBuilder) {
        switch (versionOf(body)) {
            case V1:
                return messageBuilder
                        .withHeader(headerBuilder.build())
                        .withPayload(body)
                        .build();
            case V2:
                try {
                    final JsonNode json = parseRecordBody(body);
                    return messageBuilder
                            .withHeader(headerBuilder
                                    .withAttributes(attributesFrom(json))
                                    .build())
                            .withPayload(payloadFrom(json))
                            .build();
                } catch (final RuntimeException e) {
                    return messageBuilder.withHeader(headerBuilder.build()).withPayload(body).build();
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
            return defaultObjectMapper().convertValue(
                    headersJson,
                    MAP_TYPE_REFERENCE
            );
        } else {
            return emptyMap();
        }
    }

    private static String payloadFrom(final JsonNode json) {
        final JsonNode payloadJson = json.get(SYNAPSE_MSG_PAYLOAD);
        if (payloadJson == null || payloadJson.isNull()) {
            return null;
        } else if (payloadJson.isObject() || payloadJson.isArray()) {
            return payloadJson.toString();
        } else {
            final String msg = "Unexpected json node containing " + json + ": ";
            LOG.error(msg);
            throw new IllegalStateException(msg);
        }
    }

    private static JsonNode parseRecordBody(String body) {
        try {
            return defaultObjectMapper().readTree(body);
        } catch (IOException e) {
            LOG.error("Error parsing body={} from Kinesis record: {}", body, e.getMessage());
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}
