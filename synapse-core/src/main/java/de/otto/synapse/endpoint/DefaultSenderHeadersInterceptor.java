package de.otto.synapse.endpoint;

import de.otto.synapse.configuration.SynapseProperties;
import de.otto.synapse.message.DefaultHeaderAttr;
import de.otto.synapse.message.Header;
import de.otto.synapse.message.TextMessage;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Clock;
import java.util.EnumSet;
import java.util.Set;
import java.util.UUID;

import static de.otto.synapse.message.DefaultHeaderAttr.*;
import static de.otto.synapse.message.Header.copyOf;
import static de.otto.synapse.message.Message.message;

/**
 * A {@link MessageInterceptor message interceptor} that is able to add some commonly required header attributes in
 * Synapse services.
 *
 * <p>
 *     The {@code DefaultSenderHeadersInterceptor} is active by default for all
 *     {@link de.otto.synapse.endpoint.sender.MessageSenderEndpoint sender endpoints}. It can be disabled by
 *  *     setting 'synapse.sender.default-headers.enabled=false'.
 * </p>
 * <p>
 *     The property {@code spring.application.name} is used to determine the value of the
 *     {@link DefaultHeaderAttr#MSG_SENDER} header attribute. This attribute can be used to identify the
 *     origin of a message.
 * </p>
 */
public class DefaultSenderHeadersInterceptor {

    /**
     * Capabilities of the DefaultSenderHeadersInterceptor. By default, all capablities are enabled.
     */
    public enum Capability {
        /**
         *  Add a {@link DefaultHeaderAttr#MSG_SENDER} to the message header.
         *
         *  The name of the sender is configured using property 'synapse.sender.name' or 'spring.application.name'
         */
        SENDER_NAME,
        /**
         * Add a {@link DefaultHeaderAttr#MSG_ID} to the message header.
         *
         * The value of the header is a UUID.
         */
        MESSAGE_ID,
        /**
         * Add a {@link DefaultHeaderAttr#MSG_SENDER_TS} to the message header.
         */
        TIMESTAMP
    }

    private final Set<Capability> capabilities;
    private final String senderName;
    private final Clock clock;

    /**
     * Creates a new DefaultSenderHeadersInterceptor with default configuration.
     * <p>
     *     By default, all {@link Capability capabilites} are enabled.
     * </p>
     * @param synapseProperties the properties used to configure the interceptor
     */
    public DefaultSenderHeadersInterceptor(final SynapseProperties synapseProperties) {
        this(synapseProperties, EnumSet.allOf(Capability.class), Clock.systemDefaultZone());
    }

    /**
     * Creates an instance of DefaultSenderHeadersInterceptor with enhanced configuration options.
     *
     * @param synapseProperties the properties used to configure the interceptor
     * @param capabilities the enabled capabilities
     * @param clock the clock used to generate timestamp attributes
     */
    public DefaultSenderHeadersInterceptor(final SynapseProperties synapseProperties,
                                           final Set<Capability> capabilities,
                                           final Clock clock) {
        this.senderName = synapseProperties.getSender().getName();
        this.capabilities = synapseProperties.getSender().getDefaultHeaders().isEnabled()
                ? capabilities
                : EnumSet.noneOf(Capability.class);

        this.clock = clock;
    }

    @Nullable
    @de.otto.synapse.annotation.MessageInterceptor(endpointType = EndpointType.SENDER)
    public TextMessage addDefaultHeaders(@Nonnull TextMessage message) {

        final Header.Builder headers = copyOf(message.getHeader());

        if (capabilities.contains(Capability.SENDER_NAME)) {
            headers.withAttribute(MSG_SENDER, senderName);
        }
        if (capabilities.contains(Capability.MESSAGE_ID)) {
            headers.withAttribute(MSG_ID, UUID.randomUUID().toString());
        }
        if (capabilities.contains(Capability.TIMESTAMP)) {
            headers.withAttribute(MSG_SENDER_TS, clock.instant());
        }
        return TextMessage.of(message.getKey(), headers.build(), message.getPayload());
    }
}
