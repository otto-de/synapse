package de.otto.synapse.endpoint.sender;

public interface MessageSenderFactory {

    MessageSenderEndpoint createSenderForStream(String streamName);
}
