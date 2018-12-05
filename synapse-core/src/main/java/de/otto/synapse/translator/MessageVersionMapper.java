package de.otto.synapse.translator;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import de.otto.synapse.message.Header;
import de.otto.synapse.message.Message;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Pattern;

import static de.otto.synapse.translator.ObjectMappers.defaultObjectMapper;
import static java.util.Collections.emptyMap;
import static org.slf4j.LoggerFactory.getLogger;

public class MessageVersionMapper {

    public enum Format {
        /** record.data() only contains the message payload; no header attributes supported. */
        V1,
        /** record.data() contains version (v2), header attributes and payload in JSON format */
        V2
    }

    public static final String SYNAPSE_MSG_FORMAT = "_synapse_msg_format";

    public static final Pattern V2_PATTERN = Pattern.compile("\\{\\s*\"" + SYNAPSE_MSG_FORMAT + "\"\\s*:\\s*\"v2\".+");

    public static final String SYNAPSE_MSG_HEADERS = "_synapse_msg_headers";
    public static final String SYNAPSE_MSG_PAYLOAD = "_synapse_msg_payload";

    private static final Logger LOG = getLogger(MessageVersionMapper.class);
    private static final TypeReference<Map<String, String>> MAP_TYPE_REFERENCE = new TypeReference<Map<String, String>>() {};


    public static Message<String> messageFromBody( final String body, Message.Builder<String> messageBuilder, Header.Builder headerBuilder ) {
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

    private static Format versionOf(final String body) {
        if (body != null) {
            return V2_PATTERN.matcher(body).matches()
                    ? Format.V2
                    : Format.V1;
        } else {
            return Format.V1;
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
