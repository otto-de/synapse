package de.otto.synapse.endpoint.receiver.kinesis;

import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpoint;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpointFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;

import javax.annotation.Nonnull;
import java.time.Clock;

public class KinesisMessageLogReceiverEndpointFactory implements MessageLogReceiverEndpointFactory {

    private final MessageInterceptorRegistry interceptorRegistry;
    private final KinesisAsyncClient kinesisClient;
    private final ApplicationEventPublisher eventPublisher;
    private final Clock clock;

    @Autowired
    public KinesisMessageLogReceiverEndpointFactory(final MessageInterceptorRegistry interceptorRegistry,
                                                    final KinesisAsyncClient kinesisClient,
                                                    final ApplicationEventPublisher eventPublisher) {
        this(interceptorRegistry, kinesisClient, eventPublisher, Clock.systemDefaultZone());
    }

    public KinesisMessageLogReceiverEndpointFactory(final MessageInterceptorRegistry interceptorRegistry,
                                                    final KinesisAsyncClient kinesisClient,
                                                    final ApplicationEventPublisher eventPublisher,
                                                    final Clock clock) {
        this.interceptorRegistry = interceptorRegistry;
        this.kinesisClient = kinesisClient;
        this.eventPublisher = eventPublisher;
        this.clock = clock;
    }

    @Override
    public MessageLogReceiverEndpoint create(@Nonnull String channelName) {
        return new KinesisMessageLogReceiverEndpoint(channelName, interceptorRegistry, kinesisClient, eventPublisher, clock);
    }

}
