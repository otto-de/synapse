package de.otto.synapse.endpoint.sender.aws;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.sender.MessageSenderEndpoint;
import de.otto.synapse.endpoint.sender.MessageSenderEndpointFactory;
import de.otto.synapse.translator.JsonStringMessageTranslator;
import de.otto.synapse.translator.MessageTranslator;
import software.amazon.awssdk.services.kinesis.KinesisClient;

import javax.annotation.Nonnull;

public class KinesisMessageSenderEndpointFactory implements MessageSenderEndpointFactory {

    private final MessageInterceptorRegistry registry;
    private final MessageTranslator<String> messageTranslator;
    private final KinesisClient kinesisClient;

    public KinesisMessageSenderEndpointFactory(final MessageInterceptorRegistry registry,
                                               final ObjectMapper objectMapper,
                                               final KinesisClient kinesisClient) {
        this.registry = registry;
        this.messageTranslator = new JsonStringMessageTranslator(objectMapper);
        this.kinesisClient = kinesisClient;
    }

    @Override
    public MessageSenderEndpoint create(final @Nonnull String channelName) {
        final MessageSenderEndpoint messageSender = new KinesisMessageSender(channelName, messageTranslator, kinesisClient);
        messageSender.registerInterceptorsFrom(registry);
        return messageSender;
    }

}
