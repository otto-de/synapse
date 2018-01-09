package de.otto.edison.eventsourcing;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.eventsourcing.consumer.EventSource;
import de.otto.edison.eventsourcing.kinesis.KinesisEventSource;
import de.otto.edison.eventsourcing.kinesis.KinesisStream;
import org.slf4j.Logger;
import org.springframework.security.crypto.encrypt.TextEncryptor;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.kinesis.KinesisClient;

import java.util.Objects;

import static org.slf4j.LoggerFactory.getLogger;

@Component
public class KinesisEventSourceBuilder implements EventSourceBuilder {

    private static final Logger LOG = getLogger(KinesisEventSourceBuilder.class);

    private final ObjectMapper objectMapper;
    private final KinesisClient kinesisClient;
    private final TextEncryptor textEncryptor;

    public KinesisEventSourceBuilder(final ObjectMapper objectMapper,
                                     final KinesisClient kinesisClient,
                                     final TextEncryptor textEncryptor) {
        this.objectMapper = objectMapper;
        this.kinesisClient = kinesisClient;
        this.textEncryptor = textEncryptor;
    }

    @Override
    public EventSource buildEventSource(final String streamName) {
        Objects.requireNonNull(streamName, "stream name must not be null");
        LOG.info("Building '{}' as KinesisEventSource", streamName);
        final KinesisStream kinesisStream = new KinesisStream(kinesisClient, streamName);
        return new KinesisEventSource(kinesisStream, textEncryptor, objectMapper);
    }

}
