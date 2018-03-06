package de.otto.synapse.eventsource.aws;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import de.otto.synapse.channel.aws.KinesisMessageLog;
import de.otto.synapse.channel.aws.MessageLog;
import de.otto.synapse.eventsource.EventSource;
import de.otto.synapse.eventsource.EventSourceBuilder;
import org.slf4j.Logger;
import org.springframework.context.ApplicationEventPublisher;
import software.amazon.awssdk.services.kinesis.KinesisClient;

import java.util.Objects;
import java.util.concurrent.ExecutorService;

import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.slf4j.LoggerFactory.getLogger;

public class KinesisEventSourceBuilder implements EventSourceBuilder {

    private static final Logger LOG = getLogger(KinesisEventSourceBuilder.class);
    private static final int THREAD_COUNT = 8;

    private final ObjectMapper objectMapper;
    private final ApplicationEventPublisher eventPublisher;
    private final KinesisClient kinesisClient;

    public KinesisEventSourceBuilder(final ObjectMapper objectMapper,
                                     final ApplicationEventPublisher eventPublisher,
                                     final KinesisClient kinesisClient) {
        this.objectMapper = objectMapper;
        this.eventPublisher = eventPublisher;
        this.kinesisClient = kinesisClient;
    }

    @Override
    public EventSource buildEventSource(final String name, final String streamName) {
        Objects.requireNonNull(streamName, "stream name must not be null");
        LOG.info("Building '{}' as KinesisEventSource", streamName);
        ExecutorService executorService = newFixedThreadPool(THREAD_COUNT,
                new ThreadFactoryBuilder().setNameFormat("kinesis-message-log-%d").build());
        final MessageLog messageLog = new KinesisMessageLog(kinesisClient, streamName, executorService);
        return new KinesisEventSource(name, messageLog, eventPublisher, objectMapper);
    }

}
