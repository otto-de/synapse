package de.otto.edison.eventsourcing.inmemory;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

public class InMemoryStream {

    final Queue<Tuple<String, String>> eventQueue;

    public InMemoryStream() {
        this.eventQueue = new LinkedBlockingQueue<>();
    }

    public void send(Tuple<String, String> event) {
        eventQueue.add(event);
    }

    public Tuple<String, String> receive() {
        return eventQueue.poll();
    }

}
