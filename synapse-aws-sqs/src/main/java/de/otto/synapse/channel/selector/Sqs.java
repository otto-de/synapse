package de.otto.synapse.channel.selector;

import de.otto.synapse.channel.selector.MessageQueue;
import de.otto.synapse.channel.selector.Selector;

/**
 * {@link Selector} used to specify that the desired endpoint is a SQS message queue.
 */
public interface Sqs extends MessageQueue {}
