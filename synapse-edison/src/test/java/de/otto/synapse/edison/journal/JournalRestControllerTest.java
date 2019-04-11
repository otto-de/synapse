package de.otto.synapse.edison.journal;

import de.otto.synapse.edison.state.StateRepositoryUiController;
import de.otto.synapse.journal.Journal;
import de.otto.synapse.journal.Journals;
import de.otto.synapse.message.TextMessage;
import de.otto.synapse.messagestore.MessageStoreEntry;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.context.WebApplicationContext;

import java.util.Optional;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.webAppContextSetup;

@RunWith(SpringRunner.class)
@ActiveProfiles("test")
@ContextConfiguration(
        classes = StateRepositoryUiController.class)
@WebMvcTest(
        controllers = StateRepositoryUiController.class,
        secure = false)
@EnableAutoConfiguration
@ComponentScan(
        basePackages = {"de.otto.synapse.edison.state","de.otto.synapse.edison.journal"})
public class JournalRestControllerTest {

    @Autowired
    private WebApplicationContext context;

    @MockBean
    private Journals journals;

    private MockMvc mockMvc;

    @Before
    public void setup() {
        mockMvc = webAppContextSetup(context).build();
        final Journal journal = mock(Journal.class);
        when(journal.getJournalFor("first"))
                .thenReturn(Stream.of(
                        MessageStoreEntry.of("test", TextMessage.of("first", null)))
                );
        when(journals.containsKey("test")).thenReturn(true);
        when(journals.getJournal("test")).thenReturn(Optional.of(journal));
    }

    @Test
    public void shouldConfigureJournalRestController() {
        assertThat(context.containsBean("journalRestController"), is(true));
    }

    @Test
    public void shouldGetEmptyJournalJson() throws Exception {
        mockMvc
                .perform(
                        get("/internal/journals/test/first"))
                .andExpect(
                        status().isOk())
                .andExpect(
                        jsonPath("$.messages")
                                .value(hasSize(1)));
    }

    @Test
    public void shouldGet404ForMissingJournalJson() throws Exception {
        mockMvc
                .perform(
                        get("/internal/journals/unknown/foo").accept("application/json"))
                .andExpect(
                        status().isNotFound());
    }

}