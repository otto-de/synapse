package de.otto.synapse.messagestore;

import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.message.Message;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Deque;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static de.otto.synapse.channel.ChannelPosition.fromHorizon;
import static de.otto.synapse.channel.ChannelPosition.merge;

/**
 * Concurrent in-memory implementation of a MessageStore that is storing all messages in insertion order.
 */
@ThreadSafe
public class InMemoryMessageStore implements MessageStore {

    private final Deque<Message<?>> messages = new ConcurrentLinkedDeque<>();
    private final AtomicReference<ChannelPosition> latestChannelPosition = new AtomicReference<>(fromHorizon());

    @Override
    public void add(final Message<?> message) {
        messages.add(message);
        latestChannelPosition.updateAndGet(previous -> {
            final Optional<ChannelPosition> optionalChannelPosition = message.getHeader().getChannelPosition();
            return optionalChannelPosition
                    .map(messageChannelPosition -> merge(previous, messageChannelPosition))
                    .orElse(previous);
        });
    }

    @Override
    public ChannelPosition getLatestChannelPosition() {
        return latestChannelPosition.get();
    }

    @Override
    public Stream<Message<?>> stream() {
        return messages.stream();
    }

    @Override
    public int size() {
        return messages.size();
    }
}
