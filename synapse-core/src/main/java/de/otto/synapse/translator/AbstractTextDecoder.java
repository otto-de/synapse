package de.otto.synapse.translator;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import de.otto.synapse.message.Header;
import de.otto.synapse.message.Key;
import de.otto.synapse.message.TextMessage;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import static de.otto.synapse.message.Header.copyOf;
import static de.otto.synapse.translator.ObjectMappers.currentObjectMapper;
import static java.util.Collections.emptyMap;
import static org.slf4j.LoggerFactory.getLogger;

public abstract class AbstractTextDecoder<T> implements Decoder<T> {

    private static final Logger LOG = getLogger(AbstractTextDecoder.class);

    private static final TypeReference<Map<String, String>> MAP_TYPE_REFERENCE = new TypeReference<Map<String, String>>() {};

    protected TextMessage decode(final Key prototypeKey,
                                 final Header prototypeHeader,
                                 final String body) {
        switch (MessageFormat.versionOf(body)) {
            case V1:
                return TextMessage.of(prototypeKey, prototypeHeader, body);
            case V2:
                try {
                    final JsonNode json = parseRecordBody(body);
                    return TextMessage.of(
                            keyFrom(json).orElse(prototypeKey),
                            copyOf(prototypeHeader)
                                    .withAttributes(attributesFrom(json))
                                    .build(),
                            payloadFrom(json));
                } catch (final RuntimeException e) {
                    LOG.error("Exception caught while parsing record {}: {}", body, e.getMessage());
                    return TextMessage.of(prototypeKey, prototypeHeader, body);
                }
            default:
                throw new IllegalStateException("Unsupported message format: " + body);
        }
    }

    private static Map<String, String> attributesFrom(final JsonNode json) {
        final JsonNode headersJson = json.get(MessageFormat.SYNAPSE_MSG_HEADERS);
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
        final JsonNode keyNode = json.get(MessageFormat.SYNAPSE_MSG_KEY);
        if (keyNode == null || keyNode.isNull()) {
            return Optional.empty();
        } else if (keyNode.isObject()) {
            final String partitionKey = keyNode.get(MessageFormat.SYNAPSE_MSG_PARTITIONKEY).textValue();
            final String compactionKey = keyNode.get(MessageFormat.SYNAPSE_MSG_COMPACTIONKEY).textValue();
            return Optional.of(Key.of(partitionKey, compactionKey));
        } else {
            final String msg = "Unexpected json node containing " + json + ": ";
            LOG.error(msg);
            throw new IllegalStateException(msg);
        }
    }

    private static String payloadFrom(final JsonNode json) {
        final JsonNode payloadJson = json.get(MessageFormat.SYNAPSE_MSG_PAYLOAD);
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
