package de.otto.edison.eventsourcing.inmemory;

import de.otto.edison.eventsourcing.event.EventBody;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class InMemoryStream {

    private final BlockingQueue<EventBody<String>> eventQueue;

    public InMemoryStream() {
        this.eventQueue = new LinkedBlockingQueue<>();
    }

    public void send(EventBody<String> event) {
        eventQueue.add(event);
    }

    public EventBody<String> receive() {
        try {
            return eventQueue.take();
        } catch (InterruptedException e) {
            // return null when shutting down
            return null;
        }
    }

}
