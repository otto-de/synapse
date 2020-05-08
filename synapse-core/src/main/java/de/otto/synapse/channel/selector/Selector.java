package de.otto.synapse.channel.selector;

/**
 * Selector used to select one of possibly multiple available {@link de.otto.synapse.endpoint.MessageEndpoint message endpoints}
 * in case of annotation-driven configuration of {@link de.otto.synapse.endpoint.sender.MessageSenderEndpoint message senders},
 * or {@link de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpoint message-log receivers}.
 *
 * Selectors are supported by several Synapse annotations:
 * <ul>
 *     <li>{@link de.otto.synapse.annotation.EnableEventSource}</li>
 *     <li>{@link de.otto.synapse.annotation.EnableMessageLogReceiverEndpoint}</li>
 *     <li>{@link de.otto.synapse.annotation.EnableMessageSenderEndpoint}</li>
 * </ul>
 * <p>
 *     <strong>Example:</strong>
 * </p>
 * <pre><code>
 * {@literal @}Configuration
 * {@literal @}EnableMessageSenderEndpoint channelName="my-channel", selector={@link MessageQueue MessageQueue.class}
 *
 * </code></pre>
 */
public interface Selector {
}
