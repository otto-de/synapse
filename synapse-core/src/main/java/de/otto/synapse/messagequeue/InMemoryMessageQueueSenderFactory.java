package de.otto.synapse.messagequeue;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.sender.MessageSenderEndpointFactory;
import de.otto.synapse.translator.JsonStringMessageTranslator;

import javax.annotation.Nonnull;

public class InMemoryMessageQueueSenderFactory implements MessageSenderEndpointFactory {

    private final MessageInterceptorRegistry registry;
    private final ObjectMapper objectMapper;
    private final InMemoryQueueChannels inMemoryQueueChannels;

    public InMemoryMessageQueueSenderFactory(final MessageInterceptorRegistry registry,
                                             final InMemoryQueueChannels inMemoryQueueChannels,
                                             final ObjectMapper objectMapper) {
        this.registry = registry;
        this.objectMapper = objectMapper;
        this.inMemoryQueueChannels = inMemoryQueueChannels;
    }

    @Override
    public InMemoryMessageQueueSender create(@Nonnull final String channelName) {
        final InMemoryMessageQueueSender messageSender = new InMemoryMessageQueueSender(
                new JsonStringMessageTranslator(objectMapper),
                inMemoryQueueChannels.getChannel(channelName));
        messageSender.registerInterceptorsFrom(registry);
        return messageSender;
    }
}
