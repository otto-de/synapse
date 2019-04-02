package de.otto.synapse.edison.state;

import de.otto.edison.navigation.NavBar;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.context.WebApplicationContext;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.webAppContextSetup;

@RunWith(SpringRunner.class)
@ActiveProfiles("test")
@ContextConfiguration(
        classes = StateRepositoryController.class)
@WebMvcTest(
        controllers = StateRepositoryController.class,
        secure = false)
@EnableAutoConfiguration
@ComponentScan(
        basePackages = "de.otto.synapse.edison.state")
public class StateRepositoryControllerTest {

    @Autowired
    private WebApplicationContext context;

    @Autowired
    private NavBar rightNavBar;

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
    public void shouldConfigureStateRepositoryController() {
        assertThat(context.containsBean("stateRepositoryController"), is(true));
    }

    @Test
    public void shouldGetStateRepositoriesJson() throws Exception {
        mockMvc
                .perform(
                        get("/internal/staterepositories"))
                .andExpect(
                        status().isOk())
                .andExpect(
                        jsonPath("$._links.item")
                                .value(hasSize(1)))
                .andExpect(
                        jsonPath("$._links.item[0].href")
                                .value(endsWith("/internal/staterepositories/test")));
    }

    @Test
    public void shouldGetStateRepositoryJson() throws Exception {
        mockMvc
                .perform(
                        get("/internal/staterepositories/test"))
                .andExpect(
                        status().isOk())
                .andExpect(
                        jsonPath("$._links.item")
                                .value(hasSize(2)))
                .andExpect(
                        jsonPath("$._links.item[0].href")
                                .value(endsWith("/internal/staterepositories/test/first")))
                .andExpect(
                        jsonPath("$._links.item[1].href")
                                .value(endsWith("/internal/staterepositories/test/second")));
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
    public void shouldGetEntityJson() throws Exception {
        mockMvc
                .perform(
                        get("/internal/staterepositories/test/first"))
                .andExpect(
                        status().isOk())
                .andExpect(
                        content().string("\"one\""));
    }


}