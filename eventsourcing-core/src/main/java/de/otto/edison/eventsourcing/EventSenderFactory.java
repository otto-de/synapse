package de.otto.edison.eventsourcing;

import de.otto.edison.eventsourcing.kinesis.KinesisEventSender;

public interface EventSenderFactory {

    public EventSender createSenderForStream(String streamName);
}
