package de.otto.synapse.channel;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.endpoint.receiver.AbstractMessageReceiverEndpoint;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpoint;
import de.otto.synapse.message.Message;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import static de.otto.synapse.channel.ChannelPosition.channelPosition;
import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static de.otto.synapse.message.Header.responseHeader;
import static de.otto.synapse.message.Message.message;
import static java.lang.Integer.valueOf;
import static java.time.Instant.now;
import static java.util.Collections.synchronizedList;
import static org.slf4j.LoggerFactory.getLogger;

public class InMemoryChannel extends AbstractMessageReceiverEndpoint implements MessageLogReceiverEndpoint {

    private static final Logger LOG = getLogger(InMemoryChannel.class);
    private final List<Message<String>> eventQueue;

    public InMemoryChannel(final String channelName) {
        super(channelName, new ObjectMapper());
        this.eventQueue = synchronizedList(new ArrayList<>());
    }

    public InMemoryChannel(final String channelName,
                           final ObjectMapper objectMapper) {
        super(channelName, objectMapper);
        this.eventQueue = synchronizedList(new ArrayList<>());
    }

    public void send(final Message<String> event) {
        LOG.info("Sending {} to {}", event, getChannelName());
        eventQueue.add(event);
    }

    @Nonnull
    @Override
    public ChannelPosition consume(@Nonnull final ChannelPosition startFrom,
                                   @Nonnull final Predicate<Message<?>> stopCondition) {
        boolean shouldStop;
        int pos = startFrom.shard(getChannelName()).startFrom() == StartFrom.HORIZON
                ? -1
                : valueOf(startFrom.shard(getChannelName()).position());
        do {
            if (hasMessageAfter(pos)) {
                ++pos;
                final Message<String> receivedMessage = eventQueue.get(pos);
                getMessageDispatcher().accept(message(
                        receivedMessage.getKey(),
                        responseHeader(null, now()),
                        receivedMessage.getPayload()));
                shouldStop = stopCondition.test(receivedMessage);
            } else {
                return channelPosition(fromPosition(getChannelName(), String.valueOf(pos)));
            }
        } while (!shouldStop);
        return channelPosition(fromPosition(getChannelName(), String.valueOf(pos)));
    }

    @Override
    public void stop() {
    }

    private boolean hasMessageAfter(final int pos) {
        return eventQueue.size() > (pos+1);
    }
}
