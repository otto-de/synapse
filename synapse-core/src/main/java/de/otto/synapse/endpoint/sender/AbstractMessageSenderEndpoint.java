package de.otto.synapse.endpoint.sender;

import de.otto.synapse.channel.StartFrom;
import de.otto.synapse.endpoint.AbstractMessageEndpoint;
import de.otto.synapse.endpoint.EndpointType;
import de.otto.synapse.endpoint.MessageInterceptor;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.message.Message;
import de.otto.synapse.message.TextMessage;
import de.otto.synapse.translator.MessageTranslator;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static de.otto.synapse.endpoint.EndpointType.SENDER;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.completedFuture;

/**
 * Sender-side {@code MessageEndpoint endpoint} of a Message Channel with support for {@link MessageTranslator message translation}.
 * and {@link de.otto.synapse.endpoint.MessageInterceptor interception}.
 *
 * <p>
 * <img src="http://www.enterpriseintegrationpatterns.com/img/MessageEndpointSolution.gif" alt="Message Endpoint">
 * </p>
 *
 * @see <a href="http://www.enterpriseintegrationpatterns.com/patterns/messaging/MessageEndpoint.html">EIP: Message Endpoint</a>
 * @see <a href="http://www.enterpriseintegrationpatterns.com/patterns/messaging/MessageChannel.html">EIP: Message Channel</a>
 */
public abstract class AbstractMessageSenderEndpoint extends AbstractMessageEndpoint implements MessageSenderEndpoint {

    private final MessageTranslator<TextMessage> messageTranslator;

    /**
     * Constructor used to create a new MessageEndpoint.
     *
     * @param channelName         the name of the underlying channel / stream / queue / message log.
     * @param interceptorRegistry registry used to determine {@link MessageInterceptor message interceptors} for this
     *                            endpoint.
     * @param messageTranslator   the MessageTranslator used to translate message payloads as expected by the
     *                            {@link de.otto.synapse.consumer.MessageConsumer consumers}.
     */
    public AbstractMessageSenderEndpoint(final @Nonnull String channelName,
                                         final @Nonnull String iteratorAt,
                                         final @Nonnull MessageInterceptorRegistry interceptorRegistry,
                                         final @Nonnull MessageTranslator<TextMessage> messageTranslator) {
        super(channelName, iteratorAt, interceptorRegistry);
        this.messageTranslator = messageTranslator;
    }

    public AbstractMessageSenderEndpoint(final @Nonnull String channelName,
                                  final @Nonnull MessageInterceptorRegistry interceptorRegistry,
                                  final @Nonnull MessageTranslator<TextMessage> messageTranslator) {
        super(channelName, StartFrom.HORIZON.toString(), interceptorRegistry);
        this.messageTranslator = messageTranslator;
    }

    /**
     * Sends a {@link Message} to the message channel.
     *
     * @param message the message to send
     * @param <T>     type of the message's payload
     */
    @Override
    public final <T> CompletableFuture<Void> send(@Nonnull final Message<T> message) {
        final TextMessage translatedMessage = messageTranslator.apply(message);
        final TextMessage interceptedMessage = intercept(translatedMessage);
        if (interceptedMessage != null) {
            return doSend(interceptedMessage);
        } else {
            return completedFuture(null);
        }
    }

    /**
     * Sends a stream of messages to the message channel as one or more batches, if
     * batches are supported by the infrastructure. If not, the messages are send one by one.
     *
     * @param batch a stream of messages that is sent in batched mode, if supported
     * @param <T>   the type of the message payload
     */
    @Override
    public final <T> CompletableFuture<Void> sendBatch(@Nonnull final Stream<Message<T>> batch) {
        return doSendBatch(batch
                .map(messageTranslator::apply)
                .map(this::intercept)
                .filter(Objects::nonNull));
    }

    @Nonnull
    @Override
    public final EndpointType getEndpointType() {
        return SENDER;
    }

    protected CompletableFuture<Void> doSendBatch(final @Nonnull Stream<TextMessage> batch) {
        return allOf(batch.map(this::doSend).toArray(CompletableFuture[]::new));
    }

    protected abstract CompletableFuture<Void> doSend(final @Nonnull TextMessage message);

}
