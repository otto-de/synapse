package de.otto.synapse.messagestore;

import com.google.common.annotations.Beta;
import com.google.common.collect.ImmutableSet;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.message.Header;

import javax.annotation.Nonnull;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.Iterables.getFirst;

/**
 * A repository used to store and retrieve Messages in their insertion order.
 *
 * <p>
 *     <img src="http://www.enterpriseintegrationpatterns.com/img/MessageStore.gif" alt="Message Store">
 * </p>
 * <p>
 *     When using a Message Store, we can take advantage of the asynchronous nature of a messaging infrastructure.
 *     When we send a message to a channel, we send a duplicate of the message to a special channel to be collected
 *     by the Message Store.
 * </p>
 * <p>
 *     This can be performed by the component itself or we can insert a
 *     <a href="http://www.enterpriseintegrationpatterns.com/patterns/messaging/WireTap.html">Wire Tap</a> into
 *     the channel. We can consider the secondary channel that carries a copy of the message as part of the
 *     <a href="http://www.enterpriseintegrationpatterns.com/patterns/messaging/ControlBus.html">Control Bus</a>.
 *     Sending a second message in a 'fire-and-forget' mode will not slow down the flow of the main application
 *     messages. It does, however, increase network traffic. That's why we may not store the complete message,
 *     but just a few of fields that are required for later analysis, such as a message ID, or the channel on
 *     which the message was sent and a timestamp.
 * </p>
 * @see <a href="http://www.enterpriseintegrationpatterns.com/patterns/messaging/MessageStore.html">EIP: Message Store</a>
 */
public interface MessageStore extends AutoCloseable {

    /**
     * Returns a set containing the channel names of the messages contained in the {@code MessageStore}
     * @return set of channel names
     */
    Set<String> getChannelNames();

    ImmutableSet<Index> getIndexes();

    /**
     * Returns the latest {@link ChannelPosition} of the given channel, derived from the messages contained in this
     * {@code MessageStore}.
     *
     * <p>The position is calculated by {@link ChannelPosition#merge(ChannelPosition...) merging} the
     *    {@link Header#getShardPosition() optional positions} of the messages.</p>
     *
     * <p>Messages without positions will not change the latest ChannelPosition. If no message contains
     *    position information, the returned ChannelPosition is {@link ChannelPosition#fromHorizon()}</p>
     *
     * @param channelName the name of the channel
     * @return ChannelPosition
     */
    ChannelPosition getLatestChannelPosition(final String channelName);

    @Deprecated
    default ChannelPosition getLatestChannelPosition() {
        if (getChannelNames().size() > 1) {
            throw new IllegalStateException("Deprecated method getLatestChannelPosition() called on a MessageStore containing messages from several channels: " + getChannelNames());
        } else {
            final String channelName = getFirst(getChannelNames(), "");
            return getLatestChannelPosition(channelName);
        }
    }

    /**
     * Returns a Stream of all entries contained in the MessageStore.
     *
     * <p>The stream will maintain the insertion order of the entries.</p>
     *
     * @return Stream of entries
     */
    Stream<MessageStoreEntry> stream();

    /**
     * Returns a Stream of all entries contained in the MessageStore that have are matching the given Index value
     * (optional operation).
     *
     * <p>The stream will maintain the insertion order of the entries.</p>
     *
     * @param index the Index
     * @param value the value identifying the requested messages in the specified index
     * @return stream of message-store entries matching the index or emptyStream, if the
     *         index is not known, or if there are no entries
     * @throws UnsupportedOperationException if the <tt>stream(Index,String)</tt> operation
     *         is not supported by this store
     */
    @Beta
    Stream<MessageStoreEntry> stream(final Index index, final String value);

    /**
     * Appends the specified element to the end of this list (optional
     * operation).
     *
     * <p>MessageStores that support this operation may place limitations on what
     * elements may be added to this list.  In particular, some
     * stores will refuse to add entries for more than a single
     * {@link MessageStoreEntry#getChannelName() channel}.  MessageStore
     * classes should clearly specify in their documentation any restrictions
     * on what elements may be added.
     *
     * @param entry element to be appended to this message store
     * @throws UnsupportedOperationException if the <tt>add</tt> operation
     *         is not supported by this store
     * @throws NullPointerException if the specified element is null
     * @throws IllegalArgumentException if some property of this element
     *         prevents it from being added to this list
     */
    void add(@Nonnull MessageStoreEntry entry);

    /**
     * Returns the number of messages contained in the MessageStore.
     * <p>
     *     Primarily used for testing purposes.
     *
     *     If the MessageStore can not implement this without major performance impacts (like, for example, having
     *     to download and parse huge files from S3), the method is not required to be implemented.
     * </p>
     * @return number of messages
     */
    default int size() {
        return -1;
    }

    default void close() {
    }
}
