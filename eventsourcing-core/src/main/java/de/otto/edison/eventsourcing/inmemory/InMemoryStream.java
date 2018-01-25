package de.otto.edison.eventsourcing.inmemory;

import de.otto.edison.eventsourcing.consumer.Event;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

public class InMemoryStream {

    final Queue<Event> eventQueue;

    public InMemoryStream() {
        this.eventQueue = new LinkedBlockingQueue<>();
    }

    public void send(Event event) {
        eventQueue.add(event);
    }

    public Event receive() {
        return eventQueue.poll();
    }

}
