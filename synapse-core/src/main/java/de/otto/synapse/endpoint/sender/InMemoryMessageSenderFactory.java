package de.otto.synapse.endpoint.sender;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.channel.InMemoryChannels;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.translator.JsonStringMessageTranslator;

public class InMemoryMessageSenderFactory implements MessageSenderFactory {

    private final MessageInterceptorRegistry registry;
    private final ObjectMapper objectMapper;
    private final InMemoryChannels inMemoryChannels;

    public InMemoryMessageSenderFactory(final MessageInterceptorRegistry registry,
                                        final InMemoryChannels inMemoryChannels,
                                        final ObjectMapper objectMapper) {
        this.registry = registry;
        this.objectMapper = objectMapper;
        this.inMemoryChannels = inMemoryChannels;
    }

    public MessageSenderEndpoint createSenderFor(final String channelName) {
        final InMemoryMessageSender messageSender = new InMemoryMessageSender(
                new JsonStringMessageTranslator(objectMapper),
                inMemoryChannels.getChannel(channelName));
        messageSender.registerInterceptorsFrom(registry);
        return messageSender;
    }
}
