package de.otto.synapse.endpoint.receiver.sqs;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.receiver.MessageQueueReceiverEndpoint;
import de.otto.synapse.endpoint.receiver.MessageQueueReceiverEndpointFactory;
import jakarta.annotation.Nonnull;
import org.slf4j.Logger;
import org.springframework.context.ApplicationEventPublisher;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

import java.util.concurrent.ExecutorService;

import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.slf4j.LoggerFactory.getLogger;

public class SqsMessageQueueReceiverEndpointFactory implements MessageQueueReceiverEndpointFactory {

    private static final Logger LOG = getLogger(SqsMessageQueueReceiverEndpointFactory.class);

    private final MessageInterceptorRegistry registry;
    private final SqsAsyncClient sqsAsyncClient;
    private final ApplicationEventPublisher eventPublisher;

    public SqsMessageQueueReceiverEndpointFactory(final MessageInterceptorRegistry registry,
                                                  final SqsAsyncClient sqsAsyncClient,
                                                  final ApplicationEventPublisher eventPublisher) {
        this.registry = registry;
        this.sqsAsyncClient = sqsAsyncClient;
        this.eventPublisher = eventPublisher;
    }

    @Override
    public MessageQueueReceiverEndpoint create(@Nonnull String channelName) {
        LOG.info("Auto-configuring SQS MessageQueueReceiverEndpointFactory");
        final ExecutorService executorService = newCachedThreadPool(
                new ThreadFactoryBuilder().setNameFormat("sqs-message-queue-%d").build()
        );
        return new SqsMessageQueueReceiverEndpoint(channelName, registry, sqsAsyncClient, executorService, eventPublisher);
    }
}
