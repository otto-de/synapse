package de.otto.synapse.endpoint.sender.sqs;

import de.otto.synapse.channel.selector.Selector;
import de.otto.synapse.channel.selector.Sqs;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.sender.MessageSenderEndpoint;
import de.otto.synapse.endpoint.sender.MessageSenderEndpointFactory;
import de.otto.synapse.message.TextMessage;
import de.otto.synapse.translator.MessageFormat;
import de.otto.synapse.translator.MessageTranslator;
import de.otto.synapse.translator.TextMessageTranslator;
import jakarta.annotation.Nonnull;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;

public class SqsMessageSenderEndpointFactory implements MessageSenderEndpointFactory {

    private final MessageInterceptorRegistry registry;
    private final MessageTranslator<TextMessage> messageTranslator;
    private final SqsAsyncClient sqsAsyncClient;

    public SqsMessageSenderEndpointFactory(final MessageInterceptorRegistry registry,
                                           final SqsAsyncClient sqsAsyncClient) {
        this.registry = registry;
        this.messageTranslator = new TextMessageTranslator();
        this.sqsAsyncClient = sqsAsyncClient;
    }

    @Override
    public MessageSenderEndpoint create(final @Nonnull String channelName, MessageFormat messageFormat) {
        try {
            return new SqsMessageSender(channelName, urlOf(channelName), registry, messageTranslator, sqsAsyncClient);
        } catch (Exception e) {
            throw new RuntimeException("Failed to get queueUrl for channel=" + channelName + ": " + e.getMessage(), e);
        }
    }

    @Override
    public boolean matches(Class<? extends Selector> channelSelector) {
        return channelSelector.isAssignableFrom(selector());
    }

    @Override
    public Class<? extends Selector> selector() {
        return Sqs.class;
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
