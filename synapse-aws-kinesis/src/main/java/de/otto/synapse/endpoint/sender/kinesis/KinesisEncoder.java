package de.otto.synapse.endpoint.sender.kinesis;

import de.otto.synapse.message.Message;
import de.otto.synapse.translator.Encoder;
import de.otto.synapse.translator.MessageFormat;
import de.otto.synapse.translator.TextEncoder;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;

import static java.nio.charset.StandardCharsets.UTF_8;

class KinesisEncoder implements Encoder<PutRecordsRequestEntry> {

    private final TextEncoder textEncoder;

    KinesisEncoder(final MessageFormat messageFormat) {
        this.textEncoder = new TextEncoder(messageFormat);
    }

    @Override
    public PutRecordsRequestEntry apply(final Message<String> message) {
        final String encodedMessage = textEncoder.apply(message);
        final SdkBytes sdkBytes = encodedMessage != null
                ? SdkBytes.fromString(encodedMessage, UTF_8)
                : SdkBytes.fromByteArray(new byte[]{});

        return PutRecordsRequestEntry.builder()
                .partitionKey(message.getKey().partitionKey())
                .data(sdkBytes)
                .build();
    }
}
