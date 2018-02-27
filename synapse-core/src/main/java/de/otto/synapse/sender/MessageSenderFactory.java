package de.otto.synapse.sender;

public interface MessageSenderFactory {

    MessageSender createSenderForStream(String streamName);
}
