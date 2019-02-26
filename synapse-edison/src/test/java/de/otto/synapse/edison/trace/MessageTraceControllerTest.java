package de.otto.synapse.edison.trace;

import de.otto.synapse.endpoint.EndpointType;
import de.otto.synapse.message.TextMessage;
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

import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.log;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.webAppContextSetup;

@RunWith(SpringRunner.class)
@ActiveProfiles("test")
@ContextConfiguration(classes = {MessageTraceController.class})
@WebMvcTest(controllers = MessageTraceController.class, secure = false)
@EnableAutoConfiguration
@ComponentScan(
        basePackages = {"de.otto.synapse"})
public class MessageTraceControllerTest {

    @Autowired
    private WebApplicationContext context;

    @MockBean
    private MessageTrace messageTrace;

    private MockMvc mockMvc;

    @Before
    public void setup() {
        mockMvc = webAppContextSetup(context).build();
    }

    @Test
    public void shouldConfigureMessageTraceController() {
        assertThat(context.containsBean("messageTraceController"), is(true));
    }

    @Test
    public void shouldReturnMessageTrace() throws Exception {
        when(messageTrace.stream()).thenReturn(Stream.of(
                new TraceEntry("foo-channel", EndpointType.RECEIVER, TextMessage.of("foo", "foo payload")),
                new TraceEntry("foo-channel", EndpointType.RECEIVER, TextMessage.of("foobar", "foobar payload")),
                new TraceEntry("bar-channel", EndpointType.RECEIVER, TextMessage.of("bar", "bar payload")))
        );
        mockMvc
                .perform(get("/internal/messagetrace"))
                .andDo(log())
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("<dd>foo</dd>")))
                .andExpect(content().string(containsString("<pre><code>foo payload</code></pre>")))
                .andExpect(content().string(containsString("<dd>foobar</dd>")))
                .andExpect(content().string(containsString("<pre><code>foobar payload</code></pre>")))
                .andExpect(content().string(containsString("<dd>bar</dd>")))
                .andExpect(content().string(containsString("<pre><code>bar payload</code></pre>")));
    }

    @Test
    public void shouldHandleDeletions() throws Exception {
        when(messageTrace.stream()).thenReturn(Stream.of(
                new TraceEntry("foo-channel", EndpointType.RECEIVER, TextMessage.of("foo", null)))
        );
        mockMvc
                .perform(get("/internal/messagetrace"))
                .andDo(log())
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("<dd>foo</dd>")))
                .andExpect(content().string(containsString("<pre><code>null</code></pre>")));
    }

}