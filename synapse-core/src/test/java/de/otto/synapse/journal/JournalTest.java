package de.otto.synapse.journal;

import de.otto.synapse.messagestore.Index;
import de.otto.synapse.messagestore.MessageStore;
import de.otto.synapse.messagestore.MessageStoreEntry;
import org.junit.Test;

import java.util.stream.Stream;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JournalTest {

    @Test
    public void shouldGetDefaultJournalKeyOfEntityId() {
        final Journal journal = someJournal(mock(MessageStore.class));
        assertThat(journal.journalKeyOf("42"), is("42"));
    }

    @Test
    public void getJournalFor() {
        final MessageStore messageStore = mock(MessageStore.class);
        when(messageStore.stream(Index.JOURNAL_KEY, "42")).thenReturn(Stream.of(mock(MessageStoreEntry.class)));
        final Journal journal = someJournal(messageStore);
        assertThat(journal.getJournalFor("42").count(), is(1L));
    }

    private Journal someJournal(final MessageStore messageStore) {
        return new Journal() {
            @Override
            public String getName() {
                return "";
            }

            @Override
            public MessageStore getMessageStore() {
                return messageStore;
            }
        };
    }
}