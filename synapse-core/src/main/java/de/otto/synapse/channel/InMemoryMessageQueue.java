package de.otto.synapse.channel;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.endpoint.receiver.AbstractMessageReceiverEndpoint;
import de.otto.synapse.endpoint.receiver.MessageQueueReceiverEndpoint;
import de.otto.synapse.message.Message;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static de.otto.synapse.message.Header.responseHeader;
import static de.otto.synapse.message.Message.message;
import static java.time.Instant.now;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class InMemoryMessageQueue extends AbstractMessageReceiverEndpoint implements MessageQueueReceiverEndpoint {

    private final BlockingQueue<Message<String>> eventQueue;

    public InMemoryMessageQueue(final String channelName,
                                final ObjectMapper objectMapper) {
        super(channelName, objectMapper);
        this.eventQueue = new LinkedBlockingQueue<>();
    }

    public void send(final Message<String> event) {
        eventQueue.add(event);
    }

    @Override
    public void consume() {
        while (true) {
            try {
                final Message<String> receivedMessage = eventQueue.poll(100, MILLISECONDS);
                if (receivedMessage != null) {
                    getMessageDispatcher().accept(message(
                            receivedMessage.getKey(),
                            responseHeader(null, now()),
                            receivedMessage.getPayload()));
                } else {
                    return;
                }
            } catch (final InterruptedException e) {
                return;
            }
        }
    }

}
