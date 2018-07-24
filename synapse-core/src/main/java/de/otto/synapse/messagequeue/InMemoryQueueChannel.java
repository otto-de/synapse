package de.otto.synapse.messagequeue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import de.otto.synapse.endpoint.receiver.AbstractMessageReceiverEndpoint;
import de.otto.synapse.endpoint.receiver.MessageQueueReceiverEndpoint;
import de.otto.synapse.info.MessageReceiverStatus;
import de.otto.synapse.message.Message;
import org.slf4j.Logger;
import org.springframework.context.ApplicationEventPublisher;

import javax.annotation.Nonnull;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static de.otto.synapse.message.Message.message;
import static org.slf4j.LoggerFactory.getLogger;

public class InMemoryQueueChannel extends AbstractMessageReceiverEndpoint implements MessageQueueReceiverEndpoint {

    private static final Logger LOG = getLogger(InMemoryQueueChannel.class);
    private final Queue<Message<String>> eventQueue;
    private final AtomicBoolean stopSignal = new AtomicBoolean(false);

    public InMemoryQueueChannel(final String channelName) {
        super(channelName, new ObjectMapper().registerModule(new JavaTimeModule()), null);
        this.eventQueue = new LinkedList<>();
    }

    public InMemoryQueueChannel(final String channelName,
                                final ObjectMapper objectMapper,
                                final ApplicationEventPublisher eventPublisher) {
        super(channelName, objectMapper, eventPublisher);
        this.eventQueue = new LinkedList<>();
    }

    public void send(final Message<String> message) {
        LOG.info("Sending {} to {}", message, getChannelName());
        eventQueue.add(message);
    }

    @Nonnull
    @Override
    public CompletableFuture<Void> consume() {
        publishEvent(MessageReceiverStatus.STARTED, "Started InMemoryChannelQueue " + getChannelName(), null);
        return CompletableFuture.supplyAsync(() -> {
            while (!stopSignal.get()){
                final Message<String> receivedMessage = eventQueue.poll();
                if(receivedMessage != null) {
                    final Message<String> interceptedMessage = intercept(
                            message(
                                    receivedMessage.getKey(),
                                    receivedMessage.getPayload()
                            )
                    );
                    if (interceptedMessage != null) {
                        getMessageDispatcher().accept(interceptedMessage);
                    }
                }
            }
            publishEvent(MessageReceiverStatus.FINISHED, "Finished InMemoryChannelQueue " + getChannelName(), null);
            return null;
        });
    }

    @Override
    public void stop() {
        stopSignal.set(true);
    }
}
