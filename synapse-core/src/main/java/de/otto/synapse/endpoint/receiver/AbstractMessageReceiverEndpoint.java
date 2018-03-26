package de.otto.synapse.endpoint.receiver;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.consumer.MessageDispatcher;
import de.otto.synapse.endpoint.AbstractMessageEndpoint;
import de.otto.synapse.endpoint.MessageInterceptor;

import javax.annotation.Nonnull;

/*+
 * Receiver-side {@code MessageEndpoint endpoint} of a Message Channel
 * <p>
 *     <img src="http://www.enterpriseintegrationpatterns.com/img/MessageEndpointSolution.gif" alt="Message Endpoint">
 * </p>
 */
public class AbstractMessageReceiverEndpoint extends AbstractMessageEndpoint implements MessageReceiverEndpoint {

    private final MessageDispatcher messageDispatcher;

    public AbstractMessageReceiverEndpoint(final @Nonnull String channelName,
                                           final @Nonnull ObjectMapper objectMapper) {
        super(channelName);
        messageDispatcher = new MessageDispatcher(objectMapper);
    }

    public AbstractMessageReceiverEndpoint(final @Nonnull String channelName,
                                           final @Nonnull MessageInterceptor messageInterceptor,
                                           final @Nonnull ObjectMapper objectMapper) {
        super(channelName, messageInterceptor);
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

    // TODO: <T> void register(MessageConsumer<TypeReference<T>> getMessageDispatcher, TypeReference<T> payloadType);

    /**
     * Returns the MessageDispatcher that is used to dispatch messages.
     *
     * @return MessageDispatcher
     */
    public final MessageDispatcher getMessageDispatcher() {
        return messageDispatcher;
    }
}
