package de.otto.synapse.edison.state;

import de.otto.synapse.journal.JournalRegistry;
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
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
        basePackages = "de.otto.synapse.edison.state")
public class StateRepositoryRestControllerTest {

    @Autowired
    private WebApplicationContext context;

    @MockBean
    private JournalRegistry journals;

    private MockMvc mockMvc;

    @Before
    public void setup() {
        mockMvc = webAppContextSetup(context).build();
    }

    @Test
    public void shouldConfigureStateRepositoryRestController() {
        assertThat(context.containsBean("stateRepositoryRestController"), is(true));
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
    public void shouldGetEntityJson() throws Exception {
        mockMvc
                .perform(
                        get("/internal/staterepositories/test/first"))
                .andExpect(
                        status().isOk())
                .andExpect(
                        content().string("{\"entity\":\"one\",\"_links\":{\"self\":{\"href\":\"http://localhost/internal/test/staterepositories/first\"},\"collection\":{\"href\":\"http://localhost/internal/staterepositories/test{?page,pageSize}\",\"templated\":true}}}"));
    }

    @Test
    public void shouldGet404ForMissingStateRepositoryJson() throws Exception {
        mockMvc
                .perform(
                        get("/internal/staterepositories/unknown").accept("application/json"))
                .andExpect(
                        status().isNotFound());
    }

}