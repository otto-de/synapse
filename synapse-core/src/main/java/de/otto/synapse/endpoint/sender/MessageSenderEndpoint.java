package de.otto.synapse.endpoint.sender;

import de.otto.synapse.endpoint.EndpointType;
import de.otto.synapse.endpoint.MessageEndpoint;
import de.otto.synapse.message.Message;
import de.otto.synapse.translator.MessageTranslator;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.stream.Stream;

import static de.otto.synapse.endpoint.EndpointType.SENDER;

/**
 * Sender-side {@code MessageEndpoint endpoint} of a Message Channel with support for {@link MessageTranslator message translation}.
 * and {@link de.otto.synapse.endpoint.MessageInterceptor interception}.
 *
 * <p>
 *     <img src="http://www.enterpriseintegrationpatterns.com/img/MessageEndpointSolution.gif" alt="Message Endpoint">
 * </p>
 *
 * @see <a href="http://www.enterpriseintegrationpatterns.com/patterns/messaging/MessageEndpoint.html">EIP: Message Endpoint</a>
 * @see <a href="http://www.enterpriseintegrationpatterns.com/patterns/messaging/MessageChannel.html">EIP: Message Channel</a>
 */
public abstract class MessageSenderEndpoint extends MessageEndpoint {

    private final MessageTranslator<String> messageTranslator;

    /**
     * Constructor used to create a new MessageEndpoint.
     *
     * @param channelName the name of the underlying channel / stream / queue / message log.
     * @param messageTranslator the MessageTranslator used to translate message payloads as expected by the
     * {@link de.otto.synapse.consumer.MessageConsumer consumers}.
     */
    public MessageSenderEndpoint(final String channelName,
                                 final MessageTranslator<String> messageTranslator) {
        super(channelName);
        this.messageTranslator = messageTranslator;
    }

    /**
     * Sends a {@link Message} to the message channel.
     *
     * @param message the message to send
     * @param <T> type of the message's payload
     */
    public final <T> void send(final Message<T> message) {
        final Message<String> translatedMessage = messageTranslator.translate(message);
        final Message<String> interceptedMessage = intercept(translatedMessage);
        if (interceptedMessage != null) {
            doSend(interceptedMessage);
        }
    }

    /**
     * Sends a stream of messages to the message channel as one or more batches, if
     * batches are supported by the infrastructure. If not, the messages are send one by one.
     *
     * @param batch a stream of messages that is sent in batched mode, if supported
     * @param <T> the type of the message payload
     */
    public final <T> void sendBatch(final Stream<Message<T>> batch) {
        doSendBatch(batch
                .map(messageTranslator::translate)
                .map(this::intercept)
                .filter(Objects::nonNull));
    }

    @Override
    protected final EndpointType getEndpointType() {
        return SENDER;
    }

    protected void doSendBatch(final @Nonnull Stream<Message<String>> batch) {
        batch.forEach(this::doSend);
    }

    protected abstract void doSend(final @Nonnull Message<String> message);

}
