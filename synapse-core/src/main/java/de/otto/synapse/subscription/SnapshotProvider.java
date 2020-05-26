package de.otto.synapse.subscription;

import de.otto.synapse.message.Message;

import java.util.stream.Stream;

public interface SnapshotProvider {

    /**
     * The name of the subscribable messaging channel.
     *
     * @return channel name
     */
    String channelName();

    /**
     * Returns a stream containing all Messages required to get
     * the most current snapshot for the entity that is identified
     * by the given entityId
     *
     * @param entityId the identifier used to select a single entity.
     * @return Stream of messages
     */
    Stream<? extends Message<?>> snapshot(final String entityId);
}
