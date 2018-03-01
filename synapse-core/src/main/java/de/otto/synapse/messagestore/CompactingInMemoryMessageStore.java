package de.otto.synapse.messagestore;

import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.message.Message;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static de.otto.synapse.channel.ChannelPosition.fromHorizon;
import static de.otto.synapse.channel.ChannelPosition.merge;

/**
 * Concurrent in-memory implementation of a MessageStore that is compacting messages by {@link Message#getKey() key}.
 */
@ThreadSafe
public class CompactingInMemoryMessageStore implements MessageStore {

    private final ConcurrentNavigableMap<String, Message<?>> messages = new ConcurrentSkipListMap<>();
    private final AtomicReference<ChannelPosition> latestChannelPosition = new AtomicReference<>(fromHorizon());

    @Override
    public void add(final Message<?> message) {
        messages.put(message.getKey(), message);
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
        return messages.entrySet().stream().map(Map.Entry::getValue);
    }

    @Override
    public int size() {
        return messages.size();
    }
}
