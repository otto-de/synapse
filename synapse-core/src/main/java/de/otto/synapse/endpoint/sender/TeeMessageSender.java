package de.otto.synapse.endpoint.sender;

import com.google.common.collect.ImmutableList;
import de.otto.synapse.message.Message;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

/**
 * A MessageSender that is sending all messages to N delegate MessageSenders.
 */
public class TeeMessageSender implements MessageSender {

    @Nonnull
    private final ImmutableList<? extends MessageSender> endpoints;

    public TeeMessageSender(final @Nonnull ImmutableList<? extends MessageSender> endpoints) {
        this.endpoints = endpoints;
    }

    public TeeMessageSender(final @Nonnull List<? extends MessageSender> endpoints) {
        this.endpoints = ImmutableList.copyOf(endpoints);
    }

    public TeeMessageSender(final @Nonnull MessageSender... endpoints) {
        this.endpoints = ImmutableList.copyOf(endpoints);
    }

    @Override
    public CompletableFuture<Void> send(@Nonnull Message<?> message) {
        return CompletableFuture.allOf(endpoints
                .stream()
                .map(sender -> sender.send(message))
                .toArray(CompletableFuture[]::new));
    }

    @Override
    public CompletableFuture<Void> sendBatch(@Nonnull Stream<? extends Message<?>> batch) {
        return CompletableFuture.allOf(endpoints
                .stream()
                .map(sender -> sender.sendBatch(batch))
                .toArray(CompletableFuture[]::new));
    }

}
