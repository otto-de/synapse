package de.otto.synapse.endpoint;

import javax.annotation.Nonnull;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;
import static java.util.regex.Pattern.compile;

/**
 * Information about the channels that should be intercepted by a {@link MessageInterceptor}.
 */
public class MessageInterceptorRegistration {

    private static final Pattern MATCH_ALL = compile(".*");

    private final Pattern channelNamePattern;
    private final MessageInterceptor interceptor;
    private final Set<EndpointType> enabledEndpointTypes;

    /**
     * Creates a MessageInterceptorRegistration that is used to register a {@link MessageInterceptor} in all channels,
     * at the {@link de.otto.synapse.endpoint.sender.MessageSenderEndpoint sender-side} as well as at the
     * {@link de.otto.synapse.endpoint.receiver.MessageReceiverEndpoint receiver-side}.
     *
     * @param interceptor the MessageInterceptor used to intercept messages
     * @return MessageInterceptorRegistration
     */
    public static MessageInterceptorRegistration allChannelsWith(final @Nonnull MessageInterceptor interceptor) {
        return new MessageInterceptorRegistration(MATCH_ALL, interceptor, EnumSet.allOf(EndpointType.class));
    }

    /**
     * Creates a MessageInterceptorRegistration that is used to register a {@link MessageInterceptor} in all channels
     * with names matching the {@code channelNamePattern},
     * at the {@link de.otto.synapse.endpoint.sender.MessageSenderEndpoint sender-side} as well as at the
     * {@link de.otto.synapse.endpoint.receiver.MessageReceiverEndpoint receiver-side}.
     *
     * @param channelNamePattern the regexp used to match channel names. Only channels with matching names will be intercepted.
     * @param interceptor the MessageInterceptor used to intercept messages
     * @return MessageInterceptorRegistration
     */
    public static MessageInterceptorRegistration matchingChannelsWith(final @Nonnull String channelNamePattern,
                                                                      final @Nonnull MessageInterceptor interceptor) {
        return new MessageInterceptorRegistration(compile(channelNamePattern), interceptor, EnumSet.allOf(EndpointType.class));
    }

    /**
     * Creates a MessageInterceptorRegistration that is used to register a {@link MessageInterceptor} at the
     * {@link de.otto.synapse.endpoint.receiver.MessageReceiverEndpoint receiver-side} of all channels.
     *
     * @param interceptor the MessageInterceptor used to intercept messages
     * @return MessageInterceptorRegistration
     */
    public static MessageInterceptorRegistration receiverChannelsWith(final @Nonnull MessageInterceptor interceptor) {
        return new MessageInterceptorRegistration(MATCH_ALL, interceptor, EnumSet.of(EndpointType.RECEIVER));
    }

    /**
     * Creates a MessageInterceptorRegistration that is used to register a {@link MessageInterceptor} at the
     * {@link de.otto.synapse.endpoint.receiver.MessageReceiverEndpoint receiver-side} of all channels
     * with names matching the {@code channelNamePattern}.
     *
     * @param channelNamePattern the regexp used to match channel names. Only channels with matching names will be intercepted.
     * @param interceptor the MessageInterceptor used to intercept messages
     * @return MessageInterceptorRegistration
     */
    public static MessageInterceptorRegistration matchingReceiverChannelsWith(final @Nonnull String channelNamePattern,
                                                                              final @Nonnull MessageInterceptor interceptor) {
        return new MessageInterceptorRegistration(compile(channelNamePattern), interceptor, EnumSet.of(EndpointType.RECEIVER));
    }

    /**
     * Creates a MessageInterceptorRegistration that is used to register a {@link MessageInterceptor} at the
     * {@link de.otto.synapse.endpoint.sender.MessageSenderEndpoint sender-side} of all channels.
     *
     * @param interceptor the MessageInterceptor used to intercept messages
     * @return MessageInterceptorRegistration
     */
    public static MessageInterceptorRegistration senderChannelsWith(final @Nonnull MessageInterceptor interceptor) {
        return new MessageInterceptorRegistration(MATCH_ALL, interceptor, EnumSet.of(EndpointType.SENDER));
    }

    /**
     * Creates a MessageInterceptorRegistration that is used to register a {@link MessageInterceptor} at the
     * {@link de.otto.synapse.endpoint.sender.MessageSenderEndpoint sender-side} of all channels
     * with names matching the {@code channelNamePattern}.
     *
     * @param channelNamePattern the regexp used to match channel names. Only channels with matching names will be intercepted.
     * @param interceptor the MessageInterceptor used to intercept messages
     * @return MessageInterceptorRegistration
     */
    public static MessageInterceptorRegistration matchingSenderChannelsWith(final @Nonnull String channelNamePattern,
                                                                            final @Nonnull MessageInterceptor interceptor) {
        return new MessageInterceptorRegistration(compile(channelNamePattern), interceptor, EnumSet.of(EndpointType.SENDER));
    }

    private MessageInterceptorRegistration(final Pattern channelNamePattern,
                                           final MessageInterceptor interceptor,
                                           final Set<EndpointType> enabledEndpointTypes) {
        this.channelNamePattern = requireNonNull(channelNamePattern);
        this.interceptor = requireNonNull(interceptor);
        this.enabledEndpointTypes = requireNonNull(enabledEndpointTypes);
        if (enabledEndpointTypes.isEmpty()) {
            throw new IllegalArgumentException("The set of enabled endpoint types must not be unknown");
        }
    }

    /**
     * Returns the {@code MessageInterceptor} of the registration.
     *
     * @return MessageInterceptor
     */
    public MessageInterceptor getInterceptor() {
        return interceptor;
    }

    /**
     * Returns {@code true} if the registration is matching the given {@code channelName} and {@code EndpointType},
     * false otherwise.
     *
     * @param channelName the name of the channel
     * @param endpointType the {@link EndpointType} of the channel
     * @return boolean
     */
    public boolean isEnabledFor(final String channelName,
                                final EndpointType endpointType) {
        return enabledEndpointTypes.contains(endpointType) && channelNamePattern.matcher(channelName).matches();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MessageInterceptorRegistration that = (MessageInterceptorRegistration) o;
        return Objects.equals(channelNamePattern.pattern(), that.channelNamePattern.pattern()) &&
                Objects.equals(interceptor, that.interceptor) &&
                Objects.equals(enabledEndpointTypes, that.enabledEndpointTypes);
    }

    @Override
    public int hashCode() {

        return Objects.hash(channelNamePattern.pattern(), interceptor, enabledEndpointTypes);
    }

    @Override
    public String toString() {
        return "MessageInterceptorRegistration{" +
                "channelNamePattern=" + channelNamePattern.pattern() +
                ", interceptor=" + interceptor +
                ", enabledEndpointTypes=" + enabledEndpointTypes +
                '}';
    }
}
