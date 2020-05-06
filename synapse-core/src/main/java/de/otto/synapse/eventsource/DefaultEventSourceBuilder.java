package de.otto.synapse.eventsource;

import de.otto.synapse.channel.selector.MessageLog;
import de.otto.synapse.channel.selector.Selector;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpoint;
import de.otto.synapse.messagestore.MessageStore;
import de.otto.synapse.messagestore.MessageStoreFactory;

public class DefaultEventSourceBuilder implements EventSourceBuilder {

    private final MessageStoreFactory<? extends MessageStore> snapshotMessageStoreFactory;
    private final Class<? extends MessageLog> selector;

    public DefaultEventSourceBuilder(final MessageStoreFactory<? extends MessageStore> snapshotMessageStoreFactory,
                                     final Class<? extends MessageLog> selector) {
        this.snapshotMessageStoreFactory = snapshotMessageStoreFactory;
        this.selector = selector;
    }

    @Override
    public EventSource buildEventSource(MessageLogReceiverEndpoint messageLogReceiverEndpoint) {
        final String channelName = messageLogReceiverEndpoint.getChannelName();
        final MessageStore messageStore = snapshotMessageStoreFactory.createMessageStoreFor(channelName);
        return new DefaultEventSource(messageStore, messageLogReceiverEndpoint);
    }

    @Override
    public Class<? extends Selector> selector() {
        return selector;
    }
}
