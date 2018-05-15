package de.otto.synapse.endpoint.receiver;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.consumer.MessageDispatcher;
import de.otto.synapse.endpoint.EndpointType;
import de.otto.synapse.endpoint.MessageEndpoint;

import javax.annotation.Nonnull;

import static de.otto.synapse.endpoint.EndpointType.RECEIVER;

/**
 * Receiver-side {@code MessageEndpoint endpoint} of a Message Channel
 * <p>
 *     <img src="http://www.enterpriseintegrationpatterns.com/img/MessageEndpointSolution.gif" alt="Message Endpoint">
 * </p>
 */
public class MessageReceiverEndpoint extends MessageEndpoint {

    private final MessageDispatcher messageDispatcher;

    public MessageReceiverEndpoint(final @Nonnull String channelName,
                                   final @Nonnull ObjectMapper objectMapper) {
        super(channelName);
        messageDispatcher = new MessageDispatcher(objectMapper);
    }

    /**
     * Registers a MessageConsumer at the receiver endpoint.
     *
     * {@link MessageConsumer consumers} have to be thread safe as they might be called from multiple threads
     * in parallel (e.g. for kinesis streams there is one thread per shard).
     *
     * @param messageConsumer registered EventConsumer
     */
    public final void register(MessageConsumer<?> messageConsumer) {
        messageDispatcher.add(messageConsumer);
    }

    /**
     * Returns the MessageDispatcher that is used to dispatch messages.
     *
     * @return MessageDispatcher
     */
    public final MessageDispatcher getMessageDispatcher() {
        return messageDispatcher;
    }

    @Override
    protected final EndpointType getEndpointType() {
        return RECEIVER;
    }
}
