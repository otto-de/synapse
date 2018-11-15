package de.otto.synapse.endpoint.sender.kinesis;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.channel.selector.Kinesis;
import de.otto.synapse.channel.selector.Selector;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.sender.MessageSenderEndpoint;
import de.otto.synapse.endpoint.sender.MessageSenderEndpointFactory;
import de.otto.synapse.translator.JsonStringMessageTranslator;
import de.otto.synapse.translator.MessageTranslator;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;

import javax.annotation.Nonnull;

public class KinesisMessageSenderEndpointFactory implements MessageSenderEndpointFactory {

    private final MessageInterceptorRegistry registry;
    private final MessageTranslator<String> messageTranslator;
    private final KinesisAsyncClient kinesisClient;

    public KinesisMessageSenderEndpointFactory(final MessageInterceptorRegistry registry,
                                               final ObjectMapper objectMapper,
                                               final KinesisAsyncClient kinesisClient) {
        this.registry = registry;
        this.messageTranslator = new JsonStringMessageTranslator(objectMapper);
        this.kinesisClient = kinesisClient;
    }

    @Override
    public MessageSenderEndpoint create(final @Nonnull String channelName) {
        return new KinesisMessageSender(channelName, registry, messageTranslator, kinesisClient);
    }

    @Override
    public boolean matches(final Class<? extends Selector> channelSelector) {
        return channelSelector.isAssignableFrom(Kinesis.class);
    }

}
