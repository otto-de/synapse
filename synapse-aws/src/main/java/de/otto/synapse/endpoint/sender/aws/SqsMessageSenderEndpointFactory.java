package de.otto.synapse.endpoint.sender.aws;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.sender.MessageSenderEndpoint;
import de.otto.synapse.endpoint.sender.MessageSenderEndpointFactory;
import de.otto.synapse.translator.JsonStringMessageTranslator;
import de.otto.synapse.translator.MessageTranslator;
import software.amazon.awssdk.services.sqs.SQSAsyncClient;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;

import javax.annotation.Nonnull;

public class SqsMessageSenderEndpointFactory implements MessageSenderEndpointFactory {

    private final MessageInterceptorRegistry registry;
    private final MessageTranslator<String> messageTranslator;
    private final SQSAsyncClient sqsAsyncClient;

    public SqsMessageSenderEndpointFactory(final MessageInterceptorRegistry registry,
                                           final ObjectMapper objectMapper,
                                           final SQSAsyncClient sqsAsyncClient) {
        this.registry = registry;
        this.messageTranslator = new JsonStringMessageTranslator(objectMapper);
        this.sqsAsyncClient = sqsAsyncClient;
    }

    @Override
    public MessageSenderEndpoint create(final @Nonnull String channelName) {
        try {
            final String queueUrl = sqsAsyncClient.getQueueUrl(GetQueueUrlRequest
                    .builder()
                    .queueName(channelName)
                    .build())
                    .get()
                    .queueUrl();
            final MessageSenderEndpoint messageSender = new SqsMessageSender(channelName, queueUrl, messageTranslator, sqsAsyncClient);
            messageSender.registerInterceptorsFrom(registry);
            return messageSender;
        } catch (Exception e) {
            throw new RuntimeException("Failed to get queueUrl for channel=" + channelName + ": " + e.getMessage(), e);
        }
    }

}
