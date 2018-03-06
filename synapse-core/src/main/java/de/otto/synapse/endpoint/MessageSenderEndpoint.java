package de.otto.synapse.endpoint;

import de.otto.synapse.message.Message;

import java.util.stream.Stream;

/**
 * A message-sending {@code MessageEndpoint endpoint} of a Message Channel
 *
 * @see <a href="http://www.enterpriseintegrationpatterns.com/patterns/messaging/MessageChannel.html">EIP: Message Channel</a>
 */
public interface MessageSenderEndpoint extends MessageEndpoint {

    /**
     * Sends a {@link Message} to the message channel.
     *
     * @param message the message to send
     * @param <T> type of the message's payload
     */
    <T> void send(Message<T> message);

    /**
     * Sends a stream of messages to the message channel as one or more batches, if
     * batches are supported by the infrastructure. If not, the messages are send one by one.
     *
     * @param messageStream the message stream
     * @param <T> the type of the message payload
     */
    default <T> void sendBatch(final Stream<Message<T>> messageStream) {
        messageStream.forEach(this::send);
    }

}
