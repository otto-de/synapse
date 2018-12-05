package de.otto.synapse.endpoint.sender.kinesis;

import com.fasterxml.jackson.core.JsonProcessingException;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.sender.AbstractMessageSenderEndpoint;
import de.otto.synapse.message.Message;
import de.otto.synapse.translator.MessageTranslator;
import de.otto.synapse.translator.MessageVersionMapper;
import de.otto.synapse.translator.ObjectMappers;
import org.slf4j.Logger;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static com.google.common.collect.Lists.partition;
import static de.otto.synapse.translator.MessageVersionMapper.Format.V2;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.stream.Collectors.toCollection;
import static org.slf4j.LoggerFactory.getLogger;

public class KinesisMessageSender extends AbstractMessageSenderEndpoint {

    private static final Logger LOG = getLogger(KinesisMessageSender.class);

    private static final int PUT_RECORDS_BATCH_SIZE = 500;

    private final KinesisAsyncClient kinesisAsyncClient;
    private final MessageVersionMapper.Format messageFormat;

    public KinesisMessageSender(final String channelName,
                                final MessageInterceptorRegistry interceptorRegistry,
                                final MessageTranslator<String> messageTranslator,
                                final KinesisAsyncClient kinesisClient) {
        this(channelName, interceptorRegistry, messageTranslator, kinesisClient, MessageVersionMapper.Format.V1);
    }

    public KinesisMessageSender(final String channelName,
                                final MessageInterceptorRegistry interceptorRegistry,
                                final MessageTranslator<String> messageTranslator,
                                final KinesisAsyncClient kinesisClient,
                                final MessageVersionMapper.Format messageFormat) {
        super(channelName, interceptorRegistry, messageTranslator);
        this.kinesisAsyncClient = kinesisClient;
        this.messageFormat = messageFormat;
    }

    @Override
    protected CompletableFuture<Void> doSend(@Nonnull Message<String> message) {
        // TODO: Introduce a response object and return it instead of Void
        // Just because we need a CompletableFuture<Void>, no CompletableFuture<SendMessageBatchResponse>:
        return allOf(kinesisAsyncClient.putRecords(createPutRecordRequest(message)));
    }

    @Override
    protected CompletableFuture<Void> doSendBatch(@Nonnull Stream<Message<String>> messageStream) {
        final List<PutRecordsRequestEntry> entries = createPutRecordRequestEntries(messageStream);
        return allOf(
                partition(entries, PUT_RECORDS_BATCH_SIZE)
                        .stream()
                        .map(batch -> kinesisAsyncClient.putRecords(createPutRecordsRequest(batch)))
                        .toArray(CompletableFuture[]::new));
    }

    private PutRecordsRequest createPutRecordsRequest(final List<PutRecordsRequestEntry> batch) {
        return PutRecordsRequest.builder()
                .streamName(getChannelName())
                .records(batch)
                .build();
    }

    private ArrayList<PutRecordsRequestEntry> createPutRecordRequestEntries(final @Nonnull Stream<Message<String>> messageStream) {
        return messageStream
                .map(this::requestEntryFor)
                .collect(toCollection(ArrayList::new));
    }

    private PutRecordsRequest createPutRecordRequest(final @Nonnull Message<String> message) {
        return PutRecordsRequest.builder()
                .streamName(getChannelName())
                .records(requestEntryFor(message))
                .build();
    }

    private PutRecordsRequestEntry requestEntryFor(final Message<String> message) {
        final SdkBytes sdkBytesPayload;
        // TODO: V1 is default. This will change soon...
        if (V2 == messageFormat) {
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
                        jsonPayload = ObjectMappers.defaultObjectMapper().writeValueAsString(message.getPayload());
                    } catch (JsonProcessingException e) {
                        throw new IllegalArgumentException("Unable to generate message payload from " + message.getPayload() + ": " + e.getMessage(), e);
                    }
                }
            }
            String jsonHeaders;
            try {
                jsonHeaders = ObjectMappers.defaultObjectMapper().writeValueAsString(message.getHeader().getAttributes());
            } catch (final JsonProcessingException e) {
                LOG.error("Failed to convert message headers={} into JSON message format v2: {}", message.getHeader(), e.getMessage());
                jsonHeaders = "{}";
            }
            final String json = "{\"_synapse_msg_format\":\"v2\","
                    + "\"_synapse_msg_headers\":" + jsonHeaders + ","
                    + "\"_synapse_msg_payload\":" + jsonPayload + "}";
            sdkBytesPayload = SdkBytes.fromString(json, UTF_8);
        } else {
            sdkBytesPayload = message.getPayload() != null
                    ? SdkBytes.fromString(message.getPayload(), UTF_8)
                    : SdkBytes.fromByteArray(new byte[]{});
        }
        return PutRecordsRequestEntry.builder()
                .partitionKey(message.getKey())
                .data(sdkBytesPayload)
                .build();
    }


}
