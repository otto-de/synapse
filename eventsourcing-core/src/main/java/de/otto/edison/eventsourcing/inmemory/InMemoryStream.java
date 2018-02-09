package de.otto.edison.eventsourcing.inmemory;

import de.otto.edison.eventsourcing.message.Message;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class InMemoryStream {

    private final BlockingQueue<Message<String>> eventQueue;

    public InMemoryStream() {
        this.eventQueue = new LinkedBlockingQueue<>();
    }

    public void send(final Message<String> event) {
        eventQueue.add(event);
    }

    public Message<String> receive() {
        try {
            return eventQueue.take();
        } catch (InterruptedException e) {
            // return null when shutting down
            return null;
        }
    }

}
