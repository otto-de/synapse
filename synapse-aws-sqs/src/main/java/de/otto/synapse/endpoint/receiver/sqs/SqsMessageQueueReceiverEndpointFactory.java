package de.otto.synapse.endpoint.receiver.sqs;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.receiver.MessageQueueReceiverEndpoint;
import de.otto.synapse.endpoint.receiver.MessageQueueReceiverEndpointFactory;
import org.springframework.context.ApplicationEventPublisher;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

import javax.annotation.Nonnull;

public class SqsMessageQueueReceiverEndpointFactory implements MessageQueueReceiverEndpointFactory {

    private final MessageInterceptorRegistry registry;
    private final ObjectMapper objectMapper;
    private final SqsAsyncClient sqsAsyncClient;
    private final ApplicationEventPublisher eventPublisher;

    public SqsMessageQueueReceiverEndpointFactory(final MessageInterceptorRegistry registry,
                                                  final ObjectMapper objectMapper,
                                                  final SqsAsyncClient sqsAsyncClient,
                                                  final ApplicationEventPublisher eventPublisher) {
        this.registry = registry;
        this.objectMapper = objectMapper;
        this.sqsAsyncClient = sqsAsyncClient;
        this.eventPublisher = eventPublisher;
    }

    @Override
    public MessageQueueReceiverEndpoint create(@Nonnull String channelName) {
        return new SqsMessageQueueReceiverEndpoint(channelName, registry, sqsAsyncClient, objectMapper, eventPublisher);
    }
}
