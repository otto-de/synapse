package de.otto.synapse.endpoint;

import de.otto.synapse.configuration.SynapseProperties;
import de.otto.synapse.message.Header;
import de.otto.synapse.message.TextMessage;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;

import java.time.Clock;

import static de.otto.synapse.message.DefaultHeaderAttr.MSG_RECEIVER_TS;
import static de.otto.synapse.message.Header.copyOf;

/**
 * A {@link MessageInterceptor message interceptor} that is able to add some commonly required header attributes in
 * Synapse services.
 *
 * <p>
 *     The {@code DefaultReceiverHeadersInterceptor} is active by default for all
 *     {@link de.otto.synapse.endpoint.receiver.MessageReceiverEndpoint receiver endpoints}. It can be disabled by
 *     setting 'synapse.receiver.default-headers.enabled=false'.
 * </p>
 */
public class DefaultReceiverHeadersInterceptor {

    private final Clock clock;
    private final boolean enabled;

    /**
     * Creates a new DefaultReceiverHeadersInterceptor with default configuration.
     *
     * @param synapseProperties the properties used to configure the interceptor
     */
    public DefaultReceiverHeadersInterceptor(final SynapseProperties synapseProperties) {
        this(synapseProperties, Clock.systemDefaultZone());
    }

    /**
     * Creates an instance of DefaultReceiverHeadersInterceptor for testing purposes.
     *
     * @param synapseProperties the properties used to configure the interceptor
     * @param clock the clock used to generate timestamp attributes
     */
    public DefaultReceiverHeadersInterceptor(final SynapseProperties synapseProperties,
                                             final Clock clock) {
        this.enabled = synapseProperties.getReceiver().getDefaultHeaders().isEnabled();
        this.clock = clock;
    }

    @Nullable
    @de.otto.synapse.annotation.MessageInterceptor(endpointType = EndpointType.RECEIVER)
    public TextMessage addDefaultHeaders(@Nonnull TextMessage message) {

        final Header.Builder headers = copyOf(message.getHeader());

        if (enabled) {
            headers.withAttribute(MSG_RECEIVER_TS, clock.instant());
        }
        return TextMessage.of(message.getKey(), headers.build(), message.getPayload());
    }
}
