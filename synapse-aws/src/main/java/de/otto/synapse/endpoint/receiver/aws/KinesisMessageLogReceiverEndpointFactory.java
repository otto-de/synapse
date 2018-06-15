package de.otto.synapse.endpoint.receiver.aws;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpoint;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpointFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import software.amazon.awssdk.services.kinesis.KinesisClient;

import javax.annotation.Nonnull;
import java.time.Clock;

public class KinesisMessageLogReceiverEndpointFactory implements MessageLogReceiverEndpointFactory {

    private final MessageInterceptorRegistry interceptorRegistry;
    private final KinesisClient kinesisClient;
    private final ObjectMapper objectMapper;
    private final ApplicationEventPublisher eventPublisher;
    private final Clock clock;

    @Autowired
    public KinesisMessageLogReceiverEndpointFactory(final MessageInterceptorRegistry interceptorRegistry,
                                                    final KinesisClient kinesisClient,
                                                    final ObjectMapper objectMapper,
                                                    final ApplicationEventPublisher eventPublisher) {
        this(interceptorRegistry, kinesisClient, objectMapper, eventPublisher, Clock.systemDefaultZone());
    }

    public KinesisMessageLogReceiverEndpointFactory(final MessageInterceptorRegistry interceptorRegistry,
                                                    final KinesisClient kinesisClient,
                                                    final ObjectMapper objectMapper,
                                                    final ApplicationEventPublisher eventPublisher,
                                                    final Clock clock) {
        this.interceptorRegistry = interceptorRegistry;
        this.kinesisClient = kinesisClient;
        this.objectMapper = objectMapper;
        this.eventPublisher = eventPublisher;
        this.clock = clock;
    }

    @Override
    public MessageLogReceiverEndpoint create(@Nonnull String channelName) {
        final MessageLogReceiverEndpoint messageLog = new KinesisMessageLogReceiverEndpoint(channelName, kinesisClient, objectMapper, eventPublisher, clock);
        messageLog.registerInterceptorsFrom(interceptorRegistry);
        return messageLog;
    }

}
