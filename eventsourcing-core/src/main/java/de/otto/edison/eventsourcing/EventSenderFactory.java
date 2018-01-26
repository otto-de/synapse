package de.otto.edison.eventsourcing;

public interface EventSenderFactory {

    EventSender createSenderForStream(String streamName);
}
