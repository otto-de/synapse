package de.otto.synapse.messagestore;

import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.message.Header;
import de.otto.synapse.message.Message;

import java.util.stream.Stream;

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
 *     but just a few key fields that are required for later analysis, such as a message ID, or the channel on
 *     which the message was sent and a timestamp.
 * </p>
 * @see <a href="http://www.enterpriseintegrationpatterns.com/patterns/messaging/MessageStore.html">EIP: Message Store</a>
 */
public interface MessageStore {

    /* TODO: MessageStore speichert nur Message<String>. Fühlt sich noch falsch an.

    /**
     * Adds a Message to the MessageStore.
     *
     * @param message the message to add
     */
    void add(Message<String> message);

    /**
     * Returns the latest {@link ChannelPosition} of the MessageStore.
     * <p>
     *     The position is calculated by {@link ChannelPosition#merge(ChannelPosition...) merging} the
     *     {@link Header#getShardPosition() optional positions} of the messages.
     * </p>
     * <p>
     *     Messages without positions will not change the latest ChannelPosition. If no message contains
     *     position information, the returned ChannelPosition is {@link ChannelPosition#fromHorizon()}
     * </p>
     * @return ChannelPosition
     */
    ChannelPosition getLatestChannelPosition();

    /**
     * Returns a Stream of {@link Message messages} contained in the MessageStore.
     * <p>
     *     The stream will maintain the insertion order of the messages.
     * </p>
     *
     * @return Stream of messages
     */
    Stream<Message<String>> stream();

    /**
     * Returns the number of messages contained in the MessageStore.
     *
     * @return number of messages
     */
    int size();
}
