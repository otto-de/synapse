package de.otto.synapse.journal;


import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JournalsTest {

    @Test
    public void shouldAddJournal() {
        final Journals journals = new Journals();
        final Journal journal = mock(Journal.class);
        when(journal.getName()).thenReturn("test-journal");

        journals.add(journal);

        assertThat(journals.containsKey("test-journal"), is(true));
    }

    @Test(expected = IllegalStateException.class)
    public void shouldFailToAddSameJournalTwice() {
        final Journals journals = new Journals();
        final Journal first = mock(Journal.class);
        when(first.getName()).thenReturn("test-journal");
        final Journal second = mock(Journal.class);
        when(second.getName()).thenReturn("test-journal");

        journals.add(first);
        journals.add(second);
    }

    @Test
    public void shouldNotComplainToAddSameJournalTwice() {
        final Journals journals = new Journals();
        final Journal journal = mock(Journal.class);
        when(journal.getName()).thenReturn("test-journal");

        journals.add(journal);
        journals.add(journal);

        assertThat(journals.containsKey("test-journal"), is(true));
    }

    @Test
    public void shouldGetJournal() {
        final Journals journals = new Journals();
        final Journal journal = mock(Journal.class);
        when(journal.getName()).thenReturn("test-journal");

        journals.add(journal);

        assertThat(journals.getJournal("test-journal").orElse(null), is(journal));
    }

    @Test
    public void shouldGetMissingJournal() {
        final Journals journals = new Journals();

        assertThat(journals.getJournal("test-journal").isPresent(), is(false));
    }
}