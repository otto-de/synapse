package de.otto.synapse.endpoint.receiver;

import de.otto.synapse.channel.ChannelDurationBehind;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.consumer.MessageDispatcher;
import de.otto.synapse.endpoint.AbstractMessageEndpoint;
import de.otto.synapse.endpoint.EndpointType;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.info.MessageReceiverNotification;
import de.otto.synapse.info.MessageReceiverStatus;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.springframework.context.ApplicationEventPublisher;

import java.util.Objects;

import static de.otto.synapse.endpoint.EndpointType.RECEIVER;

/**
 * Receiver-side {@code MessageEndpoint endpoint} of a Message Channel
 * <p>
 *     <img src="http://www.enterpriseintegrationpatterns.com/img/MessageEndpointSolution.gif" alt="Message Endpoint">
 * </p>
 */
public class AbstractMessageReceiverEndpoint extends AbstractMessageEndpoint implements MessageReceiverEndpoint {

    private final MessageDispatcher messageDispatcher;
    private final ApplicationEventPublisher eventPublisher;

    public AbstractMessageReceiverEndpoint(final @Nonnull String channelName,
                                           final @Nonnull MessageInterceptorRegistry interceptorRegistry,
                                           final @Nullable ApplicationEventPublisher eventPublisher) {
        super(channelName, interceptorRegistry);
        messageDispatcher = new MessageDispatcher();
        this.eventPublisher = eventPublisher;
    }

    /**
     * Registers a MessageConsumer at the receiver endpoint.
     *
     * {@link MessageConsumer consumers} have to be thread safe as they might be called from multiple threads
     * in parallel (e.g. for kinesis streams there is one thread per shard).
     *
     * @param messageConsumer registered EventConsumer
     */
    @Override
    public final void register(MessageConsumer<?> messageConsumer) {
        messageDispatcher.add(messageConsumer);
    }

    /**
     * Returns the MessageDispatcher that is used to dispatch messages.
     *
     * @return MessageDispatcher
     */
    @Override
    @Nonnull
    public final MessageDispatcher getMessageDispatcher() {
        return messageDispatcher;
    }

    @Nonnull
    @Override
    public final EndpointType getEndpointType() {
        return RECEIVER;
    }

    protected void publishEvent(final @Nonnull MessageReceiverStatus status,
                                final @Nullable String message,
                                final @Nullable ChannelDurationBehind durationBehind) {
        if (eventPublisher != null) {
            MessageReceiverNotification notification = MessageReceiverNotification.builder()
                    .withChannelName(this.getChannelName())
                    .withChannelDurationBehind(durationBehind)
                    .withStatus(status)
                    .withMessage(Objects.toString(message, ""))
                    .build();
            eventPublisher.publishEvent(notification);
        }
    }

}
