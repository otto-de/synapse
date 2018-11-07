package de.otto.synapse.channel.selector;

import de.otto.synapse.channel.selector.MessageLog;
import de.otto.synapse.channel.selector.Selector;

/**
 * {@link Selector} used to specify that the desired endpoint is a Kinesis message log.
 */
public interface Kinesis extends MessageLog {}
