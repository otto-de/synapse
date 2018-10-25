package de.otto.synapse.endpoint.sender.aws;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.sender.MessageSenderEndpoint;
import de.otto.synapse.endpoint.sender.MessageSenderEndpointFactory;
import de.otto.synapse.translator.JsonStringMessageTranslator;
import de.otto.synapse.translator.MessageTranslator;
import org.slf4j.Logger;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.ListStreamsResponse;

import javax.annotation.Nonnull;

import static org.slf4j.LoggerFactory.getLogger;

public class KinesisMessageSenderEndpointFactory implements MessageSenderEndpointFactory {

    private static final Logger LOG = getLogger(KinesisMessageSenderEndpointFactory.class);

    private final MessageInterceptorRegistry registry;
    private final MessageTranslator<String> messageTranslator;
    private final KinesisAsyncClient kinesisClient;
    private final ImmutableSet<String> kinesisChannels;

    public KinesisMessageSenderEndpointFactory(final MessageInterceptorRegistry registry,
                                               final ObjectMapper objectMapper,
                                               final KinesisAsyncClient kinesisClient) {
        this.registry = registry;
        this.messageTranslator = new JsonStringMessageTranslator(objectMapper);
        this.kinesisClient = kinesisClient;
        this.kinesisChannels = ImmutableSet.copyOf(kinesisClient
                .listStreams()
                .exceptionally(throwable -> {
                    LOG.warn("Unable to fetch Kinesis channels: {}", throwable.getMessage());
                    return ListStreamsResponse.builder().build();
                })
                .join()
                .streamNames());
    }

    @Override
    public MessageSenderEndpoint create(final @Nonnull String channelName) {
        return new KinesisMessageSender(channelName, registry, messageTranslator, kinesisClient);
    }

    @Override
    public boolean supportsChannel(final String channelName) {
        return kinesisChannels.contains(channelName);
    }
}
