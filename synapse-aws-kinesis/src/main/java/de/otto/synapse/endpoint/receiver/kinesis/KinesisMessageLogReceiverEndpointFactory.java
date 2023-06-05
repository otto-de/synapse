package de.otto.synapse.endpoint.receiver.kinesis;

import de.otto.synapse.channel.selector.Kinesis;
import de.otto.synapse.channel.selector.Selector;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpoint;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpointFactory;
import jakarta.annotation.Nonnull;
import org.slf4j.Marker;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;

import java.time.Clock;
import java.util.concurrent.ExecutorService;

import static de.otto.synapse.endpoint.receiver.kinesis.KinesisMessageLogReader.DEFAULT_WAITING_TIME_ON_EMPTY_RECORDS;

public class KinesisMessageLogReceiverEndpointFactory implements MessageLogReceiverEndpointFactory {

    private final MessageInterceptorRegistry interceptorRegistry;
    private final KinesisAsyncClient kinesisClient;
    private final ApplicationEventPublisher eventPublisher;
    private final Clock clock;
    private final ExecutorService executorService;
    private final Marker marker;

    @Autowired
    public KinesisMessageLogReceiverEndpointFactory(final MessageInterceptorRegistry interceptorRegistry,
                                                    final KinesisAsyncClient kinesisClient,
                                                    final ExecutorService kinesisMessageLogExecutorService,
                                                    final ApplicationEventPublisher eventPublisher) {
        this(interceptorRegistry, kinesisClient, kinesisMessageLogExecutorService, eventPublisher, Clock.systemDefaultZone());
    }

    public KinesisMessageLogReceiverEndpointFactory(final MessageInterceptorRegistry interceptorRegistry,
                                                    final KinesisAsyncClient kinesisClient,
                                                    final ExecutorService kinesisMessageLogExecutorService,
                                                    final ApplicationEventPublisher eventPublisher,
                                                    final Clock clock) {
        this(interceptorRegistry, kinesisClient, kinesisMessageLogExecutorService, eventPublisher, clock, null);
    }

    public KinesisMessageLogReceiverEndpointFactory(final MessageInterceptorRegistry interceptorRegistry,
                                                    final KinesisAsyncClient kinesisClient,
                                                    final ExecutorService kinesisMessageLogExecutorService,
                                                    final ApplicationEventPublisher eventPublisher,
                                                    final Clock clock,
                                                    final Marker marker) {
        this.interceptorRegistry = interceptorRegistry;
        this.kinesisClient = kinesisClient;
        this.executorService = kinesisMessageLogExecutorService;
        this.eventPublisher = eventPublisher;
        this.clock = clock;
        this.marker = marker;
    }


    @Override
    public MessageLogReceiverEndpoint create(@Nonnull String channelName) {
        return new KinesisMessageLogReceiverEndpoint(channelName, interceptorRegistry, kinesisClient, executorService, eventPublisher, clock, DEFAULT_WAITING_TIME_ON_EMPTY_RECORDS, marker);
    }

    @Override
    public boolean matches(Class<? extends Selector> channelSelector) {
        return channelSelector.isAssignableFrom(selector());
    }

    @Override
    public Class<? extends Selector> selector() {
        return Kinesis.class;
    }

}
