package de.otto.synapse;

public interface MessageSenderFactory {

    MessageSender createSenderForStream(String streamName);
}
