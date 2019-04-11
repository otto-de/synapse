package de.otto.synapse.journal;

import com.google.common.collect.ImmutableSet;
import de.otto.synapse.message.TextMessage;
import de.otto.synapse.messagestore.Index;
import de.otto.synapse.messagestore.MessageStore;
import de.otto.synapse.messagestore.MessageStoreEntry;
import org.junit.Test;

import static org.mockito.Mockito.*;

public class JournalingInterceptorTest {

    @Test(expected = IllegalStateException.class)
    public void shouldFailToCreateInterceptorWithoutProperlyIndexedMessageStore() {
        final MessageStore messageStore = mock(MessageStore.class);
        when(messageStore.getIndexes()).thenReturn(ImmutableSet.of());

        final Journal journal = mock(Journal.class);
        when(journal.getMessageStore()).thenReturn(messageStore);

        new JournalingInterceptor("foo-channel", journal);
    }

    @Test
    public void shouldAddMessageToJournal() {
        final MessageStore messageStore = mock(MessageStore.class);
        when(messageStore.getIndexes()).thenReturn(ImmutableSet.of(Index.JOURNAL_KEY));

        final Journal journal = mock(Journal.class);
        when(journal.getMessageStore()).thenReturn(messageStore);

        final JournalingInterceptor interceptor = new JournalingInterceptor("foo-channel", journal);
        final TextMessage message = TextMessage.of("42", "p");

        interceptor.intercept(message);

        verify(messageStore).add(MessageStoreEntry.of("foo-channel", message));
    }
}