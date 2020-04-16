package de.otto.synapse.message;

import de.otto.synapse.annotation.EventSourceConsumer;
import de.otto.synapse.annotation.MessageQueueConsumer;
import de.otto.synapse.consumer.MessageConsumer;

import javax.annotation.Nonnull;
import java.io.Serializable;

/**
 * The Key of a {@link Message}.
 *
 * <p>
 *     Message keys are used for different purposes:
 * </p>
 * <ul>
 * <li>
 *     As identifiers for entities stored in a {@link de.otto.synapse.state.StateRepository}, database or other
 *     kind of repository.
 * </li>
 * <li>
 *     For filtering messages, for example, using {@link MessageConsumer#keyPattern()},
 *     {@link EventSourceConsumer#keyPattern()} or {@link MessageQueueConsumer#keyPattern()}.
 * </li>
 * <li>
 *     To select the partition (aka shard) used to send the message: All messages with the same
 *     {@link #partitionKey()} will be sent over the same partiion, so the ordering of theses messages can be
 *     guaranteed.
 * </li>
 * <li>
 *     To specify a key used for compaction purposes: Message compaction will reduce all messages with the same
 *     {@link #compactionKey()} to the last message. The compacted sequence of messages is stored in a
 *     {@link de.otto.synapse.messagestore.MessageStore} and used by {@link de.otto.synapse.eventsource.EventSource}
 *     to reduce startup times of services.
 * </li>
 * </ul>
 *
 * <p>
 *     Example:
 * </p>
 *
 * <p>
 *     A channel with product-update events is partitioned into two shards A and B. Product update events are separated
 *     into {@code ProductUpdated}, {@code PriceUpdated} or {@code AvailabilityUpdated}. We need to keep the ordering
 *     of all messages per product. Given some variable {@code productId}, we could sent messages using the following
 *     keys:
 * </p>
 * <ul>
 * <li>
 *     <em>ProductUpdated:</em> Key.of(productId, "ProductUpdated#" + productId)
 * </li>
 * <li>
 *     <em>PriceUpdated:</em> Key.of(productId, "PriceUpdated#" + productId)
 * </li>
 * <li>
 *     <em>AvailabilityUpdated:</em> Key.of(productId, "AvailabilityUpdated#" + productId)
 * </li>
 * </ul>
 * <p>
 *     Using the {@code productid} (or <em>entity id</em>) as a partition key will guarantee that all events of the
 *     same product will arrive at the same partition or shard, in the same order as they were sent.
 * </p>
 * <p>
 *     The event-type in combination with the entity-id as a compaction key will guarantee, that after a compaction,
 *     the latest events for product-, price- and availability-updates are available in the compacted snapshot.
 * </p>
 */
public interface Key extends Serializable {

    /**
     * Key used for messages that do not have a distinguished key.
     *
     * <p>
     *     Note, that all messages w/o key might be sent over the same partition/shard.
     * </p>
     */
    Key NO_KEY = Key.of("nil");

    /**
     * Key used for messages that do not have a distinguished key.
     *
     * <p>
     *     Note, that all messages w/o key might be sent over the same partition/shard.
     * </p>
     * @return NO_KEY
     */
    static Key of() {
        return NO_KEY;
    }

    /**
     * Creates a simple {@code Key} where the same string is used for partitioning and compaction purposes.
     *
     * <p>
     *     Simple keys are applicable if only a single kind of messages is sent over the channel. In this case,
     *     the id of the logical entity (like, for example, the product-id) is a good candidate for building keys.
     * </p>
     * <p>
     *     If the channel is sharded and different kind of messages like price-updates and availability-updates will
     *     be sent over the same channel, and if the channel is compacted, {@link #of(String, String)} must be used,
     *     otherwise data-loss after compaction, or out-of-order arrival of messages will most likely be the result.
     * </p>
     *
     * @param key the message key
     * @return SimpleKey
     */
    static Key of(final @Nonnull String key) {
        return !key.isEmpty() ? new SimpleKey(key) : NO_KEY;
    }

    /**
     * Creates a compound {@code Key}, consisting of separate partion- and compaction-keys.
     *
     * <p>
     *     Compound keys must be used instead of {@link #of(String) simple keys} if different event-types will be sent
     *     of a partitioned channel.
     * </p>
     *
     * @param partitionKey the part of the key that is used to select the partition of a channel when sending
     *                     messages. In most cases, an entity-id like, for example, a product-id is appropriate.
     * @param compactionKey the part of the key that is used for compaction purposes. A combination of event-type and
     *                      entity-id will do the job.
     * @return CompoundKey
     */
    static Key of(final @Nonnull String partitionKey,
                  final @Nonnull String compactionKey) {
        return partitionKey.equals(compactionKey)
                ? new SimpleKey(partitionKey)
                : new CompoundKey(partitionKey, compactionKey);
    }

    /**
     * Returns the part of the key that is used to select one of possibly several shards or partitions of a
     * message channel.
     *
     * <p>
     *     All messages with the same {@code partitionKey} are guaranteed to keep their ordering.
     * </p>
     *
     * @return partition key
     */
    @Nonnull String partitionKey();

    /**
     * Returns the part of the key that is used for message compaction, where a snapshot of a message log is taken by
     * only keeping the latest message for every compaction-key.
     *
     * @return compaction key
     */
    @Nonnull String compactionKey();

    boolean isCompoundKey();

}
