package de.otto.synapse.endpoint.sender.aws;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.sender.MessageSenderEndpoint;
import de.otto.synapse.endpoint.sender.MessageSenderEndpointFactory;
import de.otto.synapse.translator.JsonStringMessageTranslator;
import de.otto.synapse.translator.MessageTranslator;
import org.slf4j.Logger;
import software.amazon.awssdk.services.kinesis.KinesisClient;

import javax.annotation.Nonnull;

import static org.slf4j.LoggerFactory.getLogger;

public class KinesisMessageSenderEndpointFactory implements MessageSenderEndpointFactory {

    private static final Logger LOG = getLogger(KinesisMessageSenderEndpointFactory.class);

    private final MessageInterceptorRegistry registry;
    private final MessageTranslator<String> messageTranslator;
    private final KinesisClient kinesisClient;
    private final ImmutableSet<String> kinesisChannels;

    public KinesisMessageSenderEndpointFactory(final MessageInterceptorRegistry registry,
                                               final ObjectMapper objectMapper,
                                               final KinesisClient kinesisClient) {
        this.registry = registry;
        this.messageTranslator = new JsonStringMessageTranslator(objectMapper);
        this.kinesisClient = kinesisClient;
        final ImmutableSet.Builder<String> streamNames = ImmutableSet.builder();
        try {
            streamNames.addAll(kinesisClient
                    .listStreams()
                    .streamNames());
        } catch (final RuntimeException e) {
            LOG.warn("Unable to access Kinesis: {}", e.getMessage());
        }
        kinesisChannels = streamNames.build();
    }

    @Override
    public MessageSenderEndpoint create(final @Nonnull String channelName) {
        final MessageSenderEndpoint messageSender = new KinesisMessageSender(channelName, messageTranslator, kinesisClient);
        messageSender.registerInterceptorsFrom(registry);
        return messageSender;
    }

    @Override
    public boolean supportsChannel(final String channelName) {
        return kinesisChannels.contains(channelName);
    }
}
