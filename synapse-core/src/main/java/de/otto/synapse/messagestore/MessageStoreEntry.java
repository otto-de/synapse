package de.otto.synapse.messagestore;

import com.google.common.collect.ImmutableMap;
import de.otto.synapse.message.TextMessage;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.Objects;

/**
 * An entry in a {@link MessageStore} which is a {@link TextMessage} stored with some extra data about the origin
 * of the message.
 */
public final class MessageStoreEntry implements Serializable {

    private static final long serialVersionUID = 8581441886626974935L;

    private final @Nonnull String channelName;
    private final @Nonnull ImmutableMap<Index,String> filterValues;
    private final @Nonnull TextMessage textMessage;

    private MessageStoreEntry(final @Nonnull String channelName,
                              final @Nonnull ImmutableMap<Index,String> filterValues,
                              final @Nonnull TextMessage textMessage) {
        this.channelName = channelName;
        this.filterValues = filterValues;
        this.textMessage = textMessage;
    }

    /**
     * Creates a new entry from {@code channelName} and the {@code textMessage}.
     * 
     * @param channelName the name of the channel that was the origin of the {@link TextMessage}
     * @param textMessage the {@code TextMessage} of the {@code MessageStoreEntry}
     * @return MessageStoreEntry
     */
    public static MessageStoreEntry of(final @Nonnull String channelName,
                                       final @Nonnull TextMessage textMessage) {
        return new MessageStoreEntry(channelName, ImmutableMap.of(), textMessage);
    }

    /**
     * Creates a new entry from {@code channelName}, {@code filterValues} and the {@code textMessage}.
     *
     * <p>The {@code filterValues} parameter contains extra information about the entry. The values will
     * typically be created by one or more {@link Indexer Indexers}, when the {@code MessageStoreEntry}
     * is added to an indexing {@link MessageStore} like, for example, {@code RedisIndexedMessageStore}.</p>
     *
     * <p>Especially in distributed MessageStore implementations, with multiple consumers listening at
     * a single channel, this information can be used to fetch messages that was added to the
     * {@code MessageStore} by a single service.</p>
     *
     * @param channelName the name of the channel that was the origin of the {@link TextMessage}
     * @param filterValues extra information used to filter entries
     * @param textMessage the {@code TextMessage} of the {@code MessageStoreEntry}
     * @return MessageStoreEntry
     */
    public static MessageStoreEntry of(final @Nonnull String channelName,
                                       final @Nonnull ImmutableMap<Index,String> filterValues,
                                       final @Nonnull TextMessage textMessage) {
        return new MessageStoreEntry(channelName, filterValues, textMessage);
    }

    /**
     * Returns the channel name of the channel that was used to transmit the {@link #getTextMessage() message}.
     *
     * @return channel name
     */
    @Nonnull
    public String getChannelName() {
        return channelName;
    }

    /**
     * Extra filtering information about the entry.
     *
     *
     * <p>The values will typically be created by one or more {@link Indexer Indexers}, when the
     * {@code MessageStoreEntry} is added to an indexing {@link MessageStore} like, for example,
     * {@code RedisIndexedMessageStore}.</p>
     *
     * <p>The keys of the filter values should be derived from {@link Index#getName()} ()} of the index
     * updated by the {@code Indexer}.</p>
     *
     * @return filter values
     */
    @Nonnull
    public ImmutableMap<Index, String> getFilterValues() {
        return filterValues;
    }

    /**
     * The message transmitted over the {@link #getChannelName() channel}
     * @return message
     */
    @Nonnull
    public TextMessage getTextMessage() {
        return textMessage;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MessageStoreEntry)) return false;
        MessageStoreEntry that = (MessageStoreEntry) o;
        return channelName.equals(that.channelName) &&
                filterValues.equals(that.filterValues) &&
                textMessage.equals(that.textMessage);
    }

    @Override
    public int hashCode() {
        return Objects.hash(channelName, filterValues, textMessage);
    }

    @Override
    public String toString() {
        return "MessageStoreEntry{" +
                "channelName='" + channelName + '\'' +
                ", filterValues=" + filterValues +
                ", textMessage=" + textMessage +
                '}';
    }
}
