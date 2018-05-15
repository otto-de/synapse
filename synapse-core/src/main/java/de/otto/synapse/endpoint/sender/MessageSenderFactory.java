package de.otto.synapse.endpoint.sender;

public interface MessageSenderFactory {

    MessageSenderEndpoint createSenderFor(String channelName);
}
