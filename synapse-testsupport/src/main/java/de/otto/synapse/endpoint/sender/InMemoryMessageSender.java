package de.otto.synapse.endpoint.sender;

import de.otto.synapse.channel.InMemoryChannel;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.message.Message;
import de.otto.synapse.translator.MessageTranslator;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;

import static java.util.concurrent.CompletableFuture.completedFuture;

public class InMemoryMessageSender extends AbstractMessageSenderEndpoint {

    private final InMemoryChannel channel;

    public InMemoryMessageSender(final MessageInterceptorRegistry interceptorRegistry,
                                 final MessageTranslator<String> messageTranslator,
                                 final InMemoryChannel channel) {
        super(channel.getChannelName(), interceptorRegistry, messageTranslator);
        this.channel = channel;
    }

    @Override
    protected CompletableFuture<Void> doSend(final @Nonnull Message<String> message) {
        channel.send(message);
        return completedFuture(null);
    }

}
