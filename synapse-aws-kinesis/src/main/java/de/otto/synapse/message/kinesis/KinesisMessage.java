package de.otto.synapse.message.kinesis;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import de.otto.synapse.message.Header;
import de.otto.synapse.message.Message;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.model.Record;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;

import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static de.otto.synapse.translator.ObjectMappers.defaultObjectMapper;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyMap;
import static software.amazon.awssdk.core.SdkBytes.fromByteArray;

public class KinesisMessage {

    public enum Version {
        /** record.data() only contains the message payload; no header attributes supported. */
        V1,
        /** record.data() contains version (v2), header attributes and payload in JSON format */
        V2
    }

    public static final String SYNAPSE_MSG_FORMAT = "_synapse_msg_format";

    public static final Pattern V2_PATTERN = Pattern.compile("\\{\\s*\"" + SYNAPSE_MSG_FORMAT + "\"\\s*:\\s*\"v2\".+");

    public static final String SYNAPSE_MSG_HEADERS = "_synapse_msg_headers";
    public static final String SYNAPSE_MSG_PAYLOAD = "_synapse_msg_payload";

    private static final TypeReference<Map<String, String>> MAP_TYPE_REFERENCE = new TypeReference<Map<String, String>>() {};

    private static final SdkBytes EMPTY_SDK_BYTES_BUFFER = fromByteArray(new byte[] {});

    private static final Function<SdkBytes, String> SDK_BYTES_STRING = sdkBytes -> {
        if (sdkBytes == null || sdkBytes.equals(EMPTY_SDK_BYTES_BUFFER)) {
            return null;
        } else {
            return sdkBytes.asString(UTF_8);
        }

    };

    public static Message<String> kinesisMessage(final @Nonnull String shard,
                                                 final @Nonnull Record record) {

        try {

            final Message.Builder<String> messageBuilder = Message.builder(String.class)
                    .withKey(record.partitionKey());

            final Header.Builder headerBuilder = Header.builder()
                    .withApproximateArrivalTimestamp(record.approximateArrivalTimestamp())
                    .withShardPosition(fromPosition(shard, record.sequenceNumber()));

            final String body = SDK_BYTES_STRING.apply(record.data());

            switch (versionOf(body)) {
                case V1:
                    return messageBuilder
                            .withHeader(headerBuilder.build())
                            .withPayload(body)
                            .build();
                case V2:
                    final JsonNode json = defaultObjectMapper().readTree(body);
                    return messageBuilder
                            .withHeader(headerBuilder
                                    .withAttributes(attributesFrom(json))
                                    .build())
                            .withPayload(payloadFrom(json))
                            .build();
                default:
                    throw new IllegalStateException("Unsupported message format: " + body);
            }
        } catch (final IllegalStateException e) {
            throw e;
        } catch (final IOException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    private static Version versionOf(final String body) {
        if (body != null) {
            return V2_PATTERN.matcher(body).matches()
                    ? Version.V2
                    : Version.V1;
        } else {
            return Version.V1;
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
            throw new IllegalStateException("unexpected json node containing " + json + ": ");
        }
    }

}
