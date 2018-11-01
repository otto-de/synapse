package de.otto.synapse.channel.aws;

import de.otto.synapse.channel.Selector;
import de.otto.synapse.channel.Selectors;

/**
 * A selection of AWS-specific {@link Selector selectors} used to select between different kind of
 * AWS {@link de.otto.synapse.endpoint.sender.MessageSenderEndpoint message-sender endpoint} implementations.
 */
public class AwsSelectors extends Selectors {

    /**
     * {@link Selector} used to specify that the desired endpoint is a Kinesis message log.
     */
    public interface Kinesis extends MessageLog {}

    /**
     * {@link Selector} used to specify that the desired endpoint is a SQS message queue.
     */
    public interface Sqs extends MessageQueue {}

    /**
     * No need to instantiate Selectors.
     */
    protected AwsSelectors() {}

}
