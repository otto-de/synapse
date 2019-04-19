package de.otto.synapse.edison.state;

import de.otto.edison.navigation.NavBar;
import de.otto.synapse.journal.Journal;
import de.otto.synapse.journal.JournalRegistry;
import de.otto.synapse.message.TextMessage;
import de.otto.synapse.messagestore.MessageStoreEntry;
import org.hamcrest.Matchers;
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
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
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
        basePackages = {"de.otto.synapse.edison.state", "de.otto.synapse.journal"})
public class StateRepositoryUiControllerTest {

    @Autowired
    private WebApplicationContext context;

    @Autowired
    private NavBar rightNavBar;

    @MockBean
    private JournalRegistry journals;

    private MockMvc mockMvc;

    @Before
    public void setup() {
        mockMvc = webAppContextSetup(context).build();
    }

    @Test
    public void shouldRegisterNavBar() {
        assertThat(rightNavBar.getItems(), hasSize(1));
        assertThat(rightNavBar.getItems().get(0).getTitle(), is("StateRepository: test"));
        assertThat(rightNavBar.getItems().get(0).getPosition(), is(15));
        assertThat(rightNavBar.getItems().get(0).getLink(), is("/internal/staterepositories/test"));
    }

    @Test
    public void shouldConfigureStateRepositoryUiController() {
        assertThat(context.containsBean("stateRepositoryUiController"), is(true));
    }


    @Test
    public void shouldLinkToJournal() throws Exception {
        final Journal journal = mock(Journal.class);
        when(journal.getJournalFor("first"))
                .thenReturn(Stream.of(
                        MessageStoreEntry.of("test", TextMessage.of("first", null)))
                );
        when(journals.hasJournal("test")).thenReturn(true);
        when(journals.getJournal("test")).thenReturn(Optional.of(journal));
        mockMvc
                .perform(
                        get("/internal/staterepositories/test/first").accept("text/html"))
                .andExpect(
                        status().isOk())
                .andExpect(
                        model().attribute("journaled", is(true))
                )
                .andExpect(
                        content().string(Matchers.containsString("<a class=\"btn btn-default btn-xs\" href=\"/internal/journals/test/first\" role=\"button\">Open Journal</a>")));
    }

    @Test
    public void shouldGetStateRepositoryEntityHtml() throws Exception {
        mockMvc
                .perform(
                        get("/internal/staterepositories/test/first").accept("text/html"))
                .andExpect(
                        status().isOk())
                .andExpect(
                        model().attribute("basePath", is("internal")))
                .andExpect(
                        model().attribute("singleEntity", is(true)))
                .andExpect(
                        model().attribute("repositoryName", is("test")))
                .andExpect(
                        model().attribute("entities", hasSize(1)))
                .andExpect(
                        model().attribute("pager", is(PagerModel.UNAVAILABLE)));
    }

    @Test
    public void shouldGetStateRepositoryHtml() throws Exception {
        mockMvc
                .perform(
                        get("/internal/staterepositories/test").accept("text/html"))
                .andExpect(
                        status().isOk())
                .andExpect(
                        model().attribute("basePath", is("internal")))
                .andExpect(
                        model().attribute("singleEntity", is(false)))
                .andExpect(
                        model().attribute("repositoryName", is("test")))
                .andExpect(
                        model().attribute("entities", hasSize(2)))
                .andExpect(
                        model().attribute("pager", is(notNullValue())));
    }

    @Test
    public void shouldGetPagedStateRepositoryHtml() throws Exception {
        mockMvc
                .perform(
                        get("/internal/staterepositories/test?page=0&pageSize=1").accept("text/html"))
                .andExpect(
                        status().isOk())
                .andExpect(
                        model().attribute("basePath", is("internal")))
                .andExpect(
                        model().attribute("singleEntity", is(false)))
                .andExpect(
                        model().attribute("repositoryName", is("test")))
                .andExpect(
                        model().attribute("entities", hasSize(1)))
                .andExpect(
                        model().attribute("pager", is(new PagerModel(
                                "http://localhost/internal/staterepositories/test?page=0&pageSize=1",
                                "http://localhost/internal/staterepositories/test?page=0&pageSize=1",
                                null,
                                "http://localhost/internal/staterepositories/test?page=1&pageSize=1",
                                "http://localhost/internal/staterepositories/test?page=1&pageSize=1"))));
    }

    @Test
    public void shouldGet404ForMissingStateRepositoryHtml() throws Exception {
        mockMvc
                .perform(
                        get("/internal/staterepositories/unknown").accept("text/html"))
                .andExpect(
                        status().isNotFound());
    }

}