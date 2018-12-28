package de.otto.synapse.messagestore;

import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.message.Message;
import net.openhft.chronicle.map.ChronicleMapBuilder;

import javax.annotation.concurrent.ThreadSafe;
import java.io.Serializable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static de.otto.synapse.channel.ChannelPosition.fromHorizon;
import static de.otto.synapse.channel.ChannelPosition.merge;

/**
 * Concurrent implementation of a MessageStore that is compacting messages by {@link Message#getKey() of}.
 * <p>
 *     The ordering of messages is guaranteed by using a (on-heap) ConcurrentSkipListSet for keys.
 * </p>
 * <p>
 *     The messages are stored in a ConcurrentMap like, for example, ChronicleMap. This way, the messages
 *     can be stored off-heap, so large numbers of messages can be stored in memory, without getting problems
 *     with Java garbage-collecting.
 * </p>
 */
@ThreadSafe
public class CompactingConcurrentMapMessageStore implements WritableMessageStore {

    private static final int DEFAULT_KEY_SIZE_BYTES = 128;
    private static final double DEFAULT_VALUE_SIZE_BYTES = 512;
    private static final long DEFAULT_ENTRY_COUNT = 1_000_00;

    private final ConcurrentSkipListSet<String> compactedAndOrderedKeys = new ConcurrentSkipListSet<>();
    private final ConcurrentMap<String, Serializable> messages;
    private final AtomicReference<ChannelPosition> latestChannelPosition = new AtomicReference<>(fromHorizon());
    private final boolean removeNullPayloadMessages;

    public CompactingConcurrentMapMessageStore() {
        this(true);
    }

    public CompactingConcurrentMapMessageStore(final boolean removeNullPayloadMessages) {
        this(removeNullPayloadMessages, ChronicleMapBuilder.of(String.class, Serializable.class)
                .averageKeySize(DEFAULT_KEY_SIZE_BYTES)
                .averageValueSize(DEFAULT_VALUE_SIZE_BYTES)
                .entries(DEFAULT_ENTRY_COUNT)
                .create());
    }

    public CompactingConcurrentMapMessageStore(final boolean removeNullPayloadMessages,
                                               final ConcurrentMap<String, Serializable> messageMap) {
        this.messages = messageMap;
        this.removeNullPayloadMessages = removeNullPayloadMessages;
    }

    @Override
    public void add(final Message<String> message) {
        final String messageKey = message.getKey().compactionKey();
        if (message.getPayload() == null && removeNullPayloadMessages) {
            messages.remove(messageKey);
            compactedAndOrderedKeys.remove(messageKey);
        } else {
            messages.put(messageKey, message);
            compactedAndOrderedKeys.add(messageKey);
        }
        latestChannelPosition.updateAndGet(previous -> {
            return message
                    .getHeader()
                    .getShardPosition()
                    .map(messageChannelPosition -> merge(previous, messageChannelPosition))
                    .orElse(previous);
        });
    }

    @Override
    public ChannelPosition getLatestChannelPosition() {
        return latestChannelPosition.get();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Stream<Message<String>> stream() {
        return compactedAndOrderedKeys.stream().map(messages::get).map(this::toStringMessage);
    }

    @SuppressWarnings("unchecked")
    private Message<String> toStringMessage(final Serializable message) {
        return (Message<String>) message;
    }

    @Override
    public int size() {
        return messages.size();
    }
}
