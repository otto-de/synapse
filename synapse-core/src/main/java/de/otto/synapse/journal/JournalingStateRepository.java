package de.otto.synapse.journal;

import de.otto.synapse.annotation.EventSourceConsumer;
import de.otto.synapse.message.Message;
import de.otto.synapse.messagestore.Index;
import de.otto.synapse.messagestore.MessageStore;
import de.otto.synapse.state.DelegatingStateRepository;
import de.otto.synapse.state.StateRepository;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A {@code StateRepository} that is also acting as a {@code Journal}.
 *
 * <p>Using {@code JournalingStateRepository} is the easiest way to get a {@code Journal} for the entites
 * that result from applying {@link Message messages} to the application's state.</p>
 *
 * <p>Using the Journal, it is possible to get a history of messages, that where changing the entities stored
 * in the {@code StateRepository} by calling {@link #getJournalFor(String)}.</p>
 *
 * <p>For every instance of {@code JournalingStateRepository}, the
 * {@link JournalingStateRepositoryBeanPostProcessor} will find all bean methods annotated with
 * {@link EventSourceConsumer} and register a {@link JournalingInterceptor}, so that consumed messages
 * will be registered in the {@link JournalingStateRepository#getMessageStore() message store}.</p>
 *
 * <p>The associated message store is required to provide a {@link Index#JOURNAL_KEY} index. See {@link Journal}
 * for details.</p>
 *
 * <p>Example:</p>
 * <pre><code>
 *     public class ProductStateRepository extends JournalingStateRepository&lt;Product&gt; {
 *
 *          public ProductStateRepository(final StateRepository&lt;Product&gt; delegate,
 *                                        final MessageStore journalMessageStore) {
 *              super(delegate, journalMessageStore);
 *          }
 *
 *          &#064;EventSourceConsumer(
 *                 eventSource = "productSource",
 *                 payloadType = ProductPayload.class
 *          )
 *
 *          public void consumeBananas(final Message&lt;Payload&gt; message) {
 *              final String entityId = message.getKey().partitionKey();
 *              put(entityIds, message.getPayload();
 *          }
 *     }
 * </code></pre>
 * @param <V> the type of the entities stored in the {@link StateRepository}
 */
public abstract class JournalingStateRepository<V> extends DelegatingStateRepository<V> implements Journal {

    private final MessageStore messageStore;

    public JournalingStateRepository(final StateRepository<V> delegate,
                                     final MessageStore messageStore) {
        super(delegate);
        checkArgument(
                messageStore.getIndexes().contains(Index.JOURNAL_KEY),
                "The provided MessageStore must be indexed for Index.JOURNAL_KEY.");
        this.messageStore = messageStore;
    }

    @Override
    public MessageStore getMessageStore() {
        return messageStore;
    }

    @Override
    public void close() throws Exception {
        messageStore.close();
        super.close();
    }

}
