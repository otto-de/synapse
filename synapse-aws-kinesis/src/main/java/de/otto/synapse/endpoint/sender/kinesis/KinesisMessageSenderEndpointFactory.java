package de.otto.synapse.endpoint.sender.kinesis;

import de.otto.synapse.channel.selector.Kinesis;
import de.otto.synapse.channel.selector.Selector;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.sender.MessageSenderEndpoint;
import de.otto.synapse.endpoint.sender.MessageSenderEndpointFactory;
import de.otto.synapse.message.TextMessage;
import de.otto.synapse.translator.MessageFormat;
import de.otto.synapse.translator.MessageTranslator;
import de.otto.synapse.translator.TextMessageTranslator;
import jakarta.annotation.Nonnull;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;

public class KinesisMessageSenderEndpointFactory implements MessageSenderEndpointFactory {

    private final MessageInterceptorRegistry registry;
    private final MessageTranslator<TextMessage> messageTranslator;
    private final KinesisAsyncClient kinesisClient;

    public KinesisMessageSenderEndpointFactory(final MessageInterceptorRegistry registry,
                                               final KinesisAsyncClient kinesisClient) {
        this.registry = registry;
        this.messageTranslator = new TextMessageTranslator();
        this.kinesisClient = kinesisClient;
    }

    @Override
    public MessageSenderEndpoint create(final @Nonnull String channelName, MessageFormat messageFormat) {
        return new KinesisMessageSender(channelName, registry, messageTranslator, kinesisClient, messageFormat);
    }

    @Override
    public boolean matches(final Class<? extends Selector> channelSelector) {
        return channelSelector.isAssignableFrom(selector());
    }

    @Override
    public Class<? extends Selector> selector() {
        return Kinesis.class;
    }

}
