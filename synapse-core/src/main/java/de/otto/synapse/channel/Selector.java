package de.otto.synapse.channel;

/**
 * Specifies the implementation used for {@link de.otto.synapse.endpoint.sender.MessageSenderEndpoint message senders}
 * in case of annotation-driven configuration using {@link de.otto.synapse.annotation.EnableMessageSenderEndpoint}.
 *
 * <p>
 *     In some cases, multiple implementations of the MessageSenderEndpoint like, for example SQS and Kinesis senders,
 *     are available in any Synapse service. If the {@code EnableMessageSenderEndpoint}
 *     annotation is used, the
 * </p>
 * <p>
 *     TODO: support selectors for message receiver annotations
 *
 *     As soon as there are multiple implementations of {@link de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpoint}
 *     and/or {@link de.otto.synapse.endpoint.receiver.MessageQueueReceiverEndpoint} available in Synapse, selectors
 *     must also supported by {@link de.otto.synapse.annotation.EnableMessageQueueReceiverEndpoint}, {@link de.otto.synapse.annotation.EnableEventSource}
 *     and the (not yet available) {@code EnableMessageLogReceiverEndpoint} annotations.
 * </p>
 * <p>
 *     <strong>Example:</strong>
 * </p>
 * <pre><code>
 * {@literal @}Configuration
 * {@literal @}EnableMessageSenderEndpoint channelName="my-channel", selector={@link Selectors Selectors}.{@link Selectors.MessageQueue MessageQueue.class}
 *
 * </code></pre>
 * @see Selectors.MessageLog
 * @see Selectors.MessageQueue
 * @see de.otto.synapse.annotation.EnableMessageSenderEndpoint
 */
public interface Selector {
}
