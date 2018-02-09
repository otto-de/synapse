package de.otto.edison.eventsourcing;

public interface MessageSenderFactory {

    MessageSender createSenderForStream(String streamName);
}
