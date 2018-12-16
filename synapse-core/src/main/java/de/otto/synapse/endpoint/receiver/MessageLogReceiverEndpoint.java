package de.otto.synapse.endpoint.receiver;

import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.ShardResponse;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.consumer.MessageDispatcher;
import de.otto.synapse.message.Message;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

import static de.otto.synapse.channel.StopCondition.shutdown;

/**
 * Receiver-side {@code MessageEndpoint endpoint} of a Message Channel that matches random-access like reading of
 * messages using {@link ChannelPosition ChannelPositions}.
 *
 * <p>
 *     <img src="http://www.enterpriseintegrationpatterns.com/img/MessageEndpointSolution.gif" alt="Message Endpoint">
 * </p>
 * <p>
 *     MessageLogReceiverEndpoints are endpoints for Publish-Subscribe Channels:
 * </p>
 * <p>
 *     <img src="http://www.enterpriseintegrationpatterns.com/img/PublishSubscribeSolution.gif" alt="Publish-Subscribe Channel">
 * </p>
 * @see <a href="http://www.enterpriseintegrationpatterns.com/patterns/messaging/PublishSubscribeChannel.html">EIP: Publish-Subscribe Channel</a>
 */
public interface MessageLogReceiverEndpoint extends MessageReceiverEndpoint {
    /**
     * Beginning at the {@code startFrom} position, messages are consumed from the message log, until the
     * {@code stopCondition} is true.
     *
     * <p>
     *     Takes zero or more messages from the channel, calls {@link #intercept(Message)} for every message, and notifies
     *     the registered consumers with the intercepted message, or drops the message, if {@code intercept} returns null.
     * </p>
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
    default CompletableFuture<ChannelPosition> consume(@Nonnull ChannelPosition startFrom) {
        return consumeUntil(startFrom, shutdown());
    }

    /**
     * Beginning at the {@code startFrom} position, messages are consumed from the message log, until the
     * {@code stopCondition} is true.
     *
     * <p>
     *     Takes zero or more messages from the channel, calls {@link #intercept(Message)} for every message, and
     *     notifies the registered consumers with the intercepted message, or drops the message, if {@code intercept}
     *     returns null.
     *</p>
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
     * @param stopCondition the predicate used to test if message consumption should be stopped
     * @return ChannelPosition
     */
    @Nonnull
    CompletableFuture<ChannelPosition> consumeUntil(@Nonnull ChannelPosition startFrom,
                                                    @Nonnull Predicate<ShardResponse> stopCondition);

    /**
     * Stops consumption of messages and shuts down the {@code MessageLogReceiverEndpoint}.
     */
    void stop();
}
