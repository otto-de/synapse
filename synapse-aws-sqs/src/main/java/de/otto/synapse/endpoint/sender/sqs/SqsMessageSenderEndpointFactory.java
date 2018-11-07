package de.otto.synapse.endpoint.sender.sqs;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.channel.selector.Selector;
import de.otto.synapse.channel.selector.Sqs;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.sender.MessageSenderEndpoint;
import de.otto.synapse.endpoint.sender.MessageSenderEndpointFactory;
import de.otto.synapse.translator.JsonStringMessageTranslator;
import de.otto.synapse.translator.MessageTranslator;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;

import javax.annotation.Nonnull;

public class SqsMessageSenderEndpointFactory implements MessageSenderEndpointFactory {

    private final MessageInterceptorRegistry registry;
    private final MessageTranslator<String> messageTranslator;
    private final SqsAsyncClient sqsAsyncClient;
    private final String messageSenderName;

    public SqsMessageSenderEndpointFactory(final MessageInterceptorRegistry registry,
                                           final ObjectMapper objectMapper,
                                           final SqsAsyncClient sqsAsyncClient,
                                           final String messageSenderName) {
        this.registry = registry;
        this.messageTranslator = new JsonStringMessageTranslator(objectMapper);
        this.sqsAsyncClient = sqsAsyncClient;
        this.messageSenderName = messageSenderName;
    }

    @Override
    public MessageSenderEndpoint create(final @Nonnull String channelName) {
        try {
            return new SqsMessageSender(channelName, urlOf(channelName), registry, messageTranslator, sqsAsyncClient, messageSenderName);
        } catch (Exception e) {
            throw new RuntimeException("Failed to get queueUrl for channel=" + channelName + ": " + e.getMessage(), e);
        }
    }

    @Override
    public boolean matches(Class<? extends Selector> channelSelector) {
        return channelSelector.isAssignableFrom(Sqs.class);
    }

    private String urlOf(final @Nonnull String channelName) {
        try {
            return sqsAsyncClient.getQueueUrl(GetQueueUrlRequest
                    .builder()
                    .queueName(channelName)
                    .build())
                    .get()
                    .queueUrl();
        } catch (Exception e) {
            throw new RuntimeException("Failed to get queueUrl for channel=" + channelName + ": " + e.getMessage(), e);
        }
    }

}
