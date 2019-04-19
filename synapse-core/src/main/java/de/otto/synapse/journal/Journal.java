package de.otto.synapse.journal;

import com.google.common.collect.ImmutableList;
import de.otto.synapse.message.Key;
import de.otto.synapse.messagestore.Index;
import de.otto.synapse.messagestore.MessageStore;
import de.otto.synapse.messagestore.MessageStoreEntry;
import de.otto.synapse.state.StateRepository;

import java.util.stream.Stream;

/**
 * A Journal contains all the messages that where leading to the current state of a single event-sourced entity
 * stored in a {@link StateRepository} or in some other kind of storage.
 *
 * <p>Messages can come from different channels. The Journal will keep track not only of the messages for a single
 * entity, but also from the originating channel for every message.</p>
 *
 * <p>The messages of a {@code Journal} will be stored in a {@link MessageStore}. The store must have a
 * {@link Index#JOURNAL_KEY journal-key index}, so the {@code Journal} is able to return the {@code Stream}
 * of messages (more precisely {@link MessageStoreEntry}) for every entity stored in the {code StateRepository}
 * when calling {@link #getJournalFor(String)}.</p>
 *
 * <p>In order to identify the messages for an entity, {@link #journalKeyOf(String)} must return the key of
 * the associated messages for a given {@code entityId}, which is the key of the stored entities: the key that
 * would be used, for example, to {@link StateRepository#get(String) get} the entity from the StateRepository.</p>
 *
 * <p>A number of default implementations of the {@code Journal} interface can be created using the {@code Journal}
 * helper {@link Journals}.</p>
 *
 * <p>Implementation hints:</p>
 *
 * <p>If you intend to write your own {@code Journal}, you need to make sure, that messages will be added
 * to the {@link Journal#getMessageStore() journal's MessageStore}, for example by registering
 * {@link JournalingInterceptor} for every message-log receiver that has to be journaled. The journal's
 * {@code MessageStore} must have a {@link Index#JOURNAL_KEY}.</p>
 *
 * <p>In most cases, the journal-key index will be calculated by just using the message's {@link Key#partitionKey()}
 * as journal key. This is the case, if the {@code partitionKey} is used as primary key of your journaled entities.</p>
 *
 * @see <a href="http://www.enterpriseintegrationpatterns.com/patterns/messaging/MessageStore.html">EIP: Message Store</a>
 */
public interface Journal {

    /**
     * The name of the journal.
     *
     * <p>In most cases, this will be equal to the name of the journaled {@link StateRepository}</p>
     *
     * @return journal name
     */
    String getName();

    /**
     * The List of channels that take influence on this Journal.
     *
     * <p>For every channel, a {@link JournalingInterceptor} is auto-configured, if the {@code Journal} is registered
     * as a Spring Bean. The interceptors will take care of {@link MessageStore#add(MessageStoreEntry) adding} incoming
     * messages to the {@link #getMessageStore() journal's MessageStore}.</p>
     *
     * @return list of channel names
     */
    ImmutableList<String> getJournaledChannels();

    /**
     * Returns the {@code MessageStore} used to store the messages of the journaled entities.
     *
     * <p>The store must be indexed for {@link Index#JOURNAL_KEY}. For every given entity, the journal-key
     * must match the key returned by {@link #journalKeyOf(String)}</p>
     *
     * @return MessageStore
     */
    MessageStore getMessageStore();

    /**
     * Calculates the journal key for a given entityId.
     *
     * <p>The returned key will be used to fetch the messages for the entity.</p>
     *
     * <p>The default implementation will simply return the {@code entityId}, assuming that the message's
     * {@link Key#partitionKey()} will be used as a primary key for stored entites.</p>
     *
     * @param entityId the key used to get the entity from a {@link StateRepository} or other entity store.
     * @return key used to identify the messages for the entity
     */
    default String journalKeyOf(final String entityId) {
        return entityId;
    }

    /**
     * Returns the {@code MessageStoreEntries} containing all messages that where modifying the state of the
     * entity identified by {@code entityId}.
     *
     * <p>The default implementation assumes, that all stored messages can be found by calling
     * {@code getMessageStore().stream(Index.JOURNAL_KEY, journalKeyOf(entityId));}.</p>
     *
     * @param entityId the key of the entity. In most cases, this will be the partitionKey of the message.
     * @return stream of MessageStoreEntry that have influenced the entity, ordered from oldest to youngest
     *         messages.
     */
    default Stream<MessageStoreEntry> getJournalFor(final String entityId) {
        return getMessageStore()
                .stream(Index.JOURNAL_KEY, journalKeyOf(entityId));
    }

}
