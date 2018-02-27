package de.otto.synapse.channel;

import de.otto.synapse.message.Message;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class InMemoryChannel {

    private final BlockingQueue<Message<String>> eventQueue;

    public InMemoryChannel() {
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
