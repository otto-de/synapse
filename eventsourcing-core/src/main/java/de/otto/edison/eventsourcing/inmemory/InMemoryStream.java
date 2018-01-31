package de.otto.edison.eventsourcing.inmemory;

import de.otto.edison.eventsourcing.event.EventBody;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

public class InMemoryStream {

    final Queue<EventBody<String>> eventQueue;

    public InMemoryStream() {
        this.eventQueue = new LinkedBlockingQueue<>();
    }

    public void send(EventBody<String> event) {
        eventQueue.add(event);
    }

    public EventBody<String> receive() {
        return eventQueue.poll();
    }

}
