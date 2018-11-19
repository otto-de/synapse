package de.otto.synapse.endpoint;

import com.google.common.collect.ImmutableMap;
import de.otto.synapse.configuration.SynapseProperties;
import de.otto.synapse.message.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Clock;
import java.util.EnumSet;
import java.util.Set;
import java.util.UUID;

import static com.google.common.collect.ImmutableMap.builder;
import static de.otto.synapse.message.Header.requestHeader;
import static de.otto.synapse.message.Message.message;

/**
 * A {@link MessageInterceptor message interceptor} that is able to add some commonly required header attributes in
 * Synapse services.
 *
 * <p>
 *     The {@code DefaultSenderHeadersInterceptor} is active by default for all
 *     {@link de.otto.synapse.endpoint.sender.MessageSenderEndpoint sender endpoints}. It can be customized
 * </p>
 * <p>
 *     The property {@code spring.application.name} is used to determine the value of the
 *     {@link DefaultSenderHeadersInterceptor#MSG_SENDER_ATTR} header attribute. This attribute can be used to identify the
 *     origin of a message.
 * </p>
 */
public class DefaultSenderHeadersInterceptor {

    /**
     * Capabilities of the DefaultSenderHeadersInterceptor. By default, all capablities are enabled.
     */
    public enum Capability {
        /**
         *  Add a {@link #MSG_SENDER_ATTR} to the message header. The name of the sender is configured using property
         *  'synapse.sender.name' or 'spring.application.name'
         */
        SENDER_NAME,
        /**
         * Add a {@link #MSG_ID_ATTR} to the message header. The value of the header is a UUID.
         */
        MESSAGE_ID,
        /**
         * Add a {@link #MSG_TIMESTAMP_ATTR} to the message header.
         */
        TIMESTAMP
    }

    /** The sender of the message */
    public static final String MSG_SENDER_ATTR = "synapse_msg_sender";
    public static final String MSG_ID_ATTR = "synapse_msg_id";
    public static final String MSG_TIMESTAMP_ATTR = "synapse_msg_timestamp";

    private final Set<Capability> capabilities;
    private final String senderName;
    private final Clock clock;

    /**
     * Creates a new DefaultSenderHeadersInterceptor with default configuration.
     * <p>
     *     By default, all {@link Capability capabilites} are enabled.
     * </p>
     * @param senderProperties the properties used to configure the interceptor
     */
    public DefaultSenderHeadersInterceptor(final SynapseProperties.Sender senderProperties) {
        this.senderName = senderProperties.getName();
        this.capabilities = EnumSet.allOf(Capability.class);
        this.clock = Clock.systemDefaultZone();
    }

    /**
     * Creates an instance of DefaultSenderHeadersInterceptor with enhanced configuration options.
     *
     * @param senderProperties the properties used to configure the interceptor
     * @param capabilities the enabled capabilities
     * @param clock the clock used to generate timestamp attributes
     */
    public DefaultSenderHeadersInterceptor(final SynapseProperties.Sender senderProperties,
                                           final Set<Capability> capabilities,
                                           final Clock clock) {
        this.senderName = senderProperties.getName();
        this.capabilities = EnumSet.allOf(Capability.class);
        this.clock = Clock.systemDefaultZone();
    }

    @Nullable
    @de.otto.synapse.annotation.MessageInterceptor(endpointType = EndpointType.SENDER)
    public Message<String> addDefaultHeaders(@Nonnull Message<String> message) {
        final ImmutableMap.Builder<String, String> attributes = builder();
        if (capabilities.contains(Capability.SENDER_NAME)) {
            attributes.put(MSG_SENDER_ATTR, senderName);
        }
        if (capabilities.contains(Capability.MESSAGE_ID)) {
            attributes.put(MSG_ID_ATTR, UUID.randomUUID().toString());
        }
        if (capabilities.contains(Capability.TIMESTAMP)) {
            attributes.put(MSG_TIMESTAMP_ATTR, clock.instant().toString());
        }
        attributes.putAll(message.getHeader().getAttributes());
        return message(message.getKey(), requestHeader(attributes.build()), message.getPayload());
    }
}
