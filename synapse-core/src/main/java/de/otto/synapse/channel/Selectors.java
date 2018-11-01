package de.otto.synapse.channel;

/**
 * A selection of generic {@link Selector selectors} used to select between different kind of
 * {@link de.otto.synapse.endpoint.sender.MessageSenderEndpoint message-sender endpoint} implementations.
 */
public class Selectors {

    /**
     * {@link Selector} used to specify that the desired endpoint is a message log like, for example, Kinesis.
     */
    public interface MessageLog extends Selector {}
    /**
     * {@link Selector} used to specify that the desired endpoint is a message queue like, for example, SQS.
     */
    public interface MessageQueue extends Selector {}

    /**
     * No need to instantiate Selectors.
     */
    protected Selectors() {}
}
