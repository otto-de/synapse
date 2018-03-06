package de.otto.synapse.sender;

import de.otto.synapse.endpoint.MessageSenderEndpoint;

public interface MessageSenderFactory {

    MessageSenderEndpoint createSenderForStream(String streamName);
}
