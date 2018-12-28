package de.otto.synapse.messagestore;

import de.otto.synapse.message.Message;

/**
 * A {@code MessageStore} that is capable to add messages.
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
public interface WritableMessageStore extends MessageStore {

    /* TODO: MessageStore speichert nur Message<String>. Fühlt sich noch falsch an.

    /**
     * Adds a Message to the MessageStore.
     *
     * @param message the message to add
     */
    void add(Message<String> message);

}
