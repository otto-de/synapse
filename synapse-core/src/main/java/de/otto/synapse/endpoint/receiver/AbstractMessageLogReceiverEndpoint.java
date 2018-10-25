package de.otto.synapse.endpoint.receiver;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import org.springframework.context.ApplicationEventPublisher;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Receiver-side {@code MessageEndpoint endpoint} of a Message Channel that supports random-access like reading of
 * messages using {@link ChannelPosition ChannelPositions}.
 *
 * <p>
 *     <img src="http://www.enterpriseintegrationpatterns.com/img/MessageEndpointSolution.gif" alt="Message Endpoint">
 * </p>
 */
public abstract class AbstractMessageLogReceiverEndpoint extends AbstractMessageReceiverEndpoint implements MessageLogReceiverEndpoint {

    public AbstractMessageLogReceiverEndpoint(final @Nonnull String channelName,
                                              final @Nonnull MessageInterceptorRegistry interceptorRegistry,
                                              final @Nonnull ObjectMapper objectMapper,
                                              final @Nullable ApplicationEventPublisher eventPublisher) {
        super(channelName, interceptorRegistry, objectMapper, eventPublisher);
    }

}
