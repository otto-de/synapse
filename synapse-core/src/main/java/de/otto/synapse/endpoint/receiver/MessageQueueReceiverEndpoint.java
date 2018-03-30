package de.otto.synapse.endpoint.receiver;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.consumer.MessageDispatcher;
import de.otto.synapse.message.Message;

import javax.annotation.Nonnull;

/**
 * Receiver-side {@code MessageEndpoint endpoint} of a Message Channel with Queue or FIFO semantics.
 *
 * <p>
 *     <img src="http://www.enterpriseintegrationpatterns.com/img/MessageEndpointSolution.gif" alt="Message Endpoint">
 * </p>
 */
public abstract class MessageQueueReceiverEndpoint extends MessageReceiverEndpoint {

    public MessageQueueReceiverEndpoint(@Nonnull String channelName, @Nonnull ObjectMapper objectMapper) {
        super(channelName, objectMapper);
    }

    /**
     * Takes zero or more messages from the channel, calls {@link #intercept(Message)} for every message, and notifies
     * the registered consumers with the intercepted message, or drops the message, if {@code intercept} returns null.
     *
     * <p>
     *     Consumption starts with the earliest available and not-yet-consumed message and finishes when either the
     *     {@code stopCondition} is met, or the application is shutting down.
     * </p>
     * <p>
     *     The {@link #register(MessageConsumer) registered} {@link MessageConsumer consumers} are used as a
     *     callback for consumed messages. A {@link MessageDispatcher} can be used as a consumer, if multiple
     *     consumers, or consumers with {@link Message#getPayload() message payloads} other than {@code String} are
     *     required.
     * </p>
     *
     */
    public abstract void consume();

}
