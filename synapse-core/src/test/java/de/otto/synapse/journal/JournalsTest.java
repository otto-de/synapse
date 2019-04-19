package de.otto.synapse.journal;


import com.google.common.collect.ImmutableList;
import de.otto.synapse.endpoint.MessageInterceptorRegistration;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.state.StateRepository;
import org.junit.Test;

import static de.otto.synapse.journal.Journals.singleChannelJournal;
import static java.util.Collections.emptyList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.*;

public class JournalsTest {

    @Test
    public void shouldAddJournal() {
        final Journal journal = singleChannelJournal("test-journal", "some-channel");
        final JournalRegistry journals = new JournalRegistry(ImmutableList.of(journal), mock(MessageInterceptorRegistry.class));

        assertThat(journals.hasJournal("test-journal"), is(true));
    }

    @Test
    public void shouldRegisterInterceptor() {
        final Journal journal = singleChannelJournal("Some Journal", "some-channel");
        final MessageInterceptorRegistry registry = mock(MessageInterceptorRegistry.class);

        new JournalRegistry(ImmutableList.of(journal), registry);

        verify(registry).register(any(MessageInterceptorRegistration.class));
    }

    @Test(expected = IllegalStateException.class)
    public void shouldFailToAddSameJournalTwice() {
        final Journal journal = singleChannelJournal("Some Journal", "some-channel");

        new JournalRegistry(ImmutableList.of(journal, journal), mock(MessageInterceptorRegistry.class));
    }

    @Test
    public void shouldGetJournal() {
        final Journal journal = singleChannelJournal("Some Journal", "some-channel");
        final MessageInterceptorRegistry registry = mock(MessageInterceptorRegistry.class);

        final JournalRegistry journals = new JournalRegistry(ImmutableList.of(journal), registry);

        assertThat(journals.getJournal("Some Journal").orElse(null), is(journal));
    }

    @Test
    public void shouldGetJournalByStateRepository() {
        final StateRepository stateRepository = mock(StateRepository.class);
        when(stateRepository.getName()).thenReturn("Some Repo");
        final Journal journal = singleChannelJournal(stateRepository, "some-channel");
        final MessageInterceptorRegistry registry = mock(MessageInterceptorRegistry.class);

        final JournalRegistry journals = new JournalRegistry(ImmutableList.of(journal), registry);

        assertThat(journals.getJournal("Some Repo").orElse(null), is(journal));
    }

    @Test
    public void shouldGetMissingJournal() {
        final JournalRegistry journals = new JournalRegistry(emptyList(), mock(MessageInterceptorRegistry.class));

        assertThat(journals.getJournal("test-journal").isPresent(), is(false));
    }
}