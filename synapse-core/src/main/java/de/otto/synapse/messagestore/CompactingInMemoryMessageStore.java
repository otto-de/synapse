package de.otto.synapse.messagestore;

import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.message.Message;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static de.otto.synapse.channel.ChannelPosition.fromHorizon;
import static de.otto.synapse.channel.ChannelPosition.merge;

/**
 * Concurrent in-memory implementation of a MessageStore that is compacting messages by {@link Message#getKey() key}.
 * <p>
 *     Messages are stored using a ConcurrentNavigableMap.
 * </p>
 */
@ThreadSafe
public class CompactingInMemoryMessageStore implements WritableMessageStore {

    private final ConcurrentNavigableMap<String, Message<String>> messages = new ConcurrentSkipListMap<>();
    private final AtomicReference<ChannelPosition> latestChannelPosition = new AtomicReference<>(fromHorizon());
    private final boolean removeNullPayloadMessages;

    public CompactingInMemoryMessageStore() {
        this.removeNullPayloadMessages = true;
    }

    public CompactingInMemoryMessageStore(final boolean removeNullPayloadMessages) {
        this.removeNullPayloadMessages = removeNullPayloadMessages;
    }

    @Override
    public void add(final Message<String> message) {
        final String messageKey = message.getHeader().getShardPosition().map(pos -> pos.shardName() + "-" + message.getKey()).orElse(message.getKey());
        if (message.getPayload() == null && removeNullPayloadMessages) {
            messages.remove(messageKey);
        } else {
            messages.put(messageKey, message);
        }
        latestChannelPosition.updateAndGet(previous -> {
            return message
                    .getHeader()
                    .getShardPosition()
                    .map(shardPosition -> merge(previous, shardPosition))
                    .orElse(previous);
        });
    }

    @Override
    public ChannelPosition getLatestChannelPosition() {
        return latestChannelPosition.get();
    }

    @Override
    public Stream<Message<String>> stream() {
        return messages.entrySet().stream().map(Map.Entry::getValue);
    }

    @Override
    public int size() {
        return messages.size();
    }
}
