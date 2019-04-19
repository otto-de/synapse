package de.otto.synapse.journal;

import de.otto.synapse.endpoint.MessageInterceptor;
import de.otto.synapse.message.TextMessage;
import de.otto.synapse.messagestore.Index;
import de.otto.synapse.messagestore.MessageStoreEntry;
import org.slf4j.Logger;

import javax.annotation.Nonnull;

import static com.google.common.base.Preconditions.checkState;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Interceptor used to add the {@link de.otto.synapse.message.Message messages} of event-sourced entities
 * to a {@link Journal}.
 */
public class JournalingInterceptor implements MessageInterceptor {

    private static final Logger LOG = getLogger(JournalingInterceptor.class);

    private final String channelName;
    private final Journal journal;

    /**
     * Creates a {@code JournalingInterceptor} for a channel, using the given {@code Journal}.
     * @param channelName the name of the journaled channel
     * @param journal the Journal used to record messages for the channel
     * @throws IllegalStateException if the Journal's MessageStore does not provide an {@link Index#JOURNAL_KEY}
     */
    public JournalingInterceptor(final @Nonnull String channelName,
                                 final @Nonnull Journal journal) {
        checkState(
                journal.getMessageStore().getIndexes().contains(Index.JOURNAL_KEY),
                "The provided MessageStore must be indexed for Index.JOURNAL_KEY.");
        this.channelName = channelName;
        this.journal = journal;
    }

    @Nonnull
    @Override
    public TextMessage intercept(final @Nonnull TextMessage message) {
        LOG.debug("Added message {} to Journal using messageStore {}", message, journal.getName());
        journal.getMessageStore().add(MessageStoreEntry.of(channelName, message));
        return message;
    }

    public Journal getJournal() {
        return journal;
    }
}
