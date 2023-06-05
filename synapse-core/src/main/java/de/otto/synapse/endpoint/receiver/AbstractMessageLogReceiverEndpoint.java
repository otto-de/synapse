package de.otto.synapse.endpoint.receiver;

import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.springframework.context.ApplicationEventPublisher;

/**
 * Receiver-side {@code MessageEndpoint endpoint} of a Message Channel that matches random-access like reading of
 * messages using {@link ChannelPosition ChannelPositions}.
 *
 * <p>
 *     <img src="http://www.enterpriseintegrationpatterns.com/img/MessageEndpointSolution.gif" alt="Message Endpoint">
 * </p>
 */
public abstract class AbstractMessageLogReceiverEndpoint extends AbstractMessageReceiverEndpoint implements MessageLogReceiverEndpoint {

    public AbstractMessageLogReceiverEndpoint(final @Nonnull String channelName,
                                              final @Nonnull MessageInterceptorRegistry interceptorRegistry,
                                              final @Nullable ApplicationEventPublisher eventPublisher) {
        super(channelName, interceptorRegistry, eventPublisher);
    }

}
