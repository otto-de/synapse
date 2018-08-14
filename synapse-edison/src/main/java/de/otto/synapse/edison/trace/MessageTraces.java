package de.otto.synapse.edison.trace;

import com.google.common.collect.ImmutableMap;
import de.otto.synapse.endpoint.receiver.AbstractMessageReceiverEndpoint;
import de.otto.synapse.messagestore.MessageStore;

import java.util.Set;

/**
 * Provides access to message traces of senders and receivers.
 */
public class MessageTraces {

    private final ImmutableMap<String, MessageStore> receiverTraces;
    private final ImmutableMap<String, MessageStore> senderTraces;

    public MessageTraces(final ImmutableMap<String, MessageStore> receiverTraces,
                         final ImmutableMap<String, MessageStore> senderTraces) {
        this.receiverTraces = receiverTraces;
        this.senderTraces = senderTraces;
    }

    /**
     * Returns the channel names of the {@link AbstractMessageReceiverEndpoint}s, where
     * traces of received messages are available.
     *
     * @return set of channel names
     */
    public Set<String> getReceiverChannels() {
        return receiverTraces.keySet();
    }

    /**
     * Returns a MessageStore containing the last messages received by the named channel.
     *
     * @param channelName the name of the channel
     * @return MessageStore or null, if there is no sender trace available for the given channel.
     */
    public MessageStore getReceiverTrace(final String channelName) {
        final MessageStore messageStore = receiverTraces.get(channelName);
        return messageStore != null ? messageStore : MessageStore.empty();
    }

    /**
     * Returns the channel names of the {@link AbstractMessageReceiverEndpoint}s, where
     * traces of sent messages are available.
     *
     * @return set of channel names
     */

    public Set<String> getSenderChannels() {
        return senderTraces.keySet();
    }

    /**
     * Returns a MessageStore containing the last messages sent to the named channel.
     *
     * @param channelName the name of the channel
     * @return MessageStore or null, if there is no sender trace available for the given channel.
     */
    public MessageStore getSenderTrace(final String channelName) {
        return senderTraces.get(channelName);
    }
}
