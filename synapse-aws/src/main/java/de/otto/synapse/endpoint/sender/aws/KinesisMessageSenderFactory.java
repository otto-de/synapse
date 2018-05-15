package de.otto.synapse.endpoint.sender.aws;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.sender.MessageSenderEndpoint;
import de.otto.synapse.endpoint.sender.MessageSenderFactory;
import de.otto.synapse.translator.JsonStringMessageTranslator;
import de.otto.synapse.translator.MessageTranslator;
import software.amazon.awssdk.services.kinesis.KinesisClient;

public class KinesisMessageSenderFactory implements MessageSenderFactory {

    private final MessageInterceptorRegistry registry;
    private final MessageTranslator<String> messageTranslator;
    private final KinesisClient kinesisClient;

    public KinesisMessageSenderFactory(final MessageInterceptorRegistry registry,
                                       final ObjectMapper objectMapper,
                                       final KinesisClient kinesisClient) {
        this.registry = registry;
        this.messageTranslator = new JsonStringMessageTranslator(objectMapper);
        this.kinesisClient = kinesisClient;
    }

    public MessageSenderEndpoint createSenderFor(final String channelName) {
        final MessageSenderEndpoint messageSender = new KinesisMessageSender(channelName, messageTranslator, kinesisClient);
        messageSender.registerInterceptorsFrom(registry);
        return messageSender;
    }

}
