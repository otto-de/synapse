package de.otto.synapse.endpoint.receiver;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.consumer.MessageDispatcher;
import de.otto.synapse.message.Message;
import org.springframework.context.ApplicationEventPublisher;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Instant;
import java.util.function.Predicate;

/**
 * Receiver-side {@code MessageEndpoint endpoint} of a Message Channel that supports random-access like reading of
 * messages using {@link ChannelPosition ChannelPositions}.
 *
 * <p>
 *     <img src="http://www.enterpriseintegrationpatterns.com/img/MessageEndpointSolution.gif" alt="Message Endpoint">
 * </p>
 */
public abstract class MessageLogReceiverEndpoint extends MessageReceiverEndpoint {

    public MessageLogReceiverEndpoint(final @Nonnull String channelName,
                                      final @Nonnull ObjectMapper objectMapper,
                                      final @Nullable ApplicationEventPublisher eventPublisher) {
        super(channelName, objectMapper, eventPublisher);
    }

    /**
     * Takes zero or more messages from the channel, calls {@link #intercept(Message)} for every message, and notifies
     * the registered consumers with the intercepted message, or drops the message, if {@code intercept} returns null.
     *
     * <p>
     *     Consumption starts with the first message <em>after</em> {@code startFrom} and finishes when either the
     *     {@code stopCondition} is met, or the application is shutting down.
     * </p>
     * <p>
     *     The returned {@code ChannelPosition} is the position of the last message that was processed by the
     *     {@code MessageLogReceiverEndpoint} - whether it was dropped or consumed.
     * </p>
     * <p>
     *     The {@link #register(MessageConsumer) registered} {@link MessageConsumer consumers} are used as a
     *     callback for consumed messages. A {@link MessageDispatcher} can be used as a consumer, if multiple
     *     consumers, or consumers with {@link Message#getPayload() message payloads} other than {@code String} are
     *     required.
     * </p>
     *
     * @param startFrom the start position used to proceed message consumption
     * @return ChannelPosition
     */
    @Nonnull
    public final ChannelPosition consume(@Nonnull ChannelPosition startFrom) {
        return consumeUntil(startFrom, Instant.MAX);
    }

    /**
     * Takes zero or more messages from the channel, calls {@link #intercept(Message)} for every message, and notifies
     * the registered consumers with the intercepted message, or drops the message, if {@code intercept} returns null.
     *
     * <p>
     *     Consumption starts with the first message <em>after</em> {@code startFrom} and finishes when either the
     *     {@code stopCondition} is met, or the application is shutting down.
     * </p>
     * <p>
     *     The returned {@code ChannelPosition} is the position of the last message that was processed by the
     *     {@code MessageLogReceiverEndpoint} - whether it was dropped or consumed.
     * </p>
     * <p>
     *     The {@link #register(MessageConsumer) registered} {@link MessageConsumer consumers} are used as a
     *     callback for consumed messages. A {@link MessageDispatcher} can be used as a consumer, if multiple
     *     consumers, or consumers with {@link Message#getPayload() message payloads} other than {@code String} are
     *     required.
     * </p>
     *
     * @param startFrom the start position used to proceed message consumption
     * @param until the arrival timestamp until the messages should be consumed
     * @return ChannelPosition
     */
    @Nonnull
    public abstract ChannelPosition consumeUntil(@Nonnull ChannelPosition startFrom,
                                                 @Nonnull Instant until);

    public abstract void stop();
}
