package de.otto.synapse.edison.history;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.context.WebApplicationContext;

import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static de.otto.synapse.message.Header.responseHeader;
import static de.otto.synapse.message.Message.message;
import static java.time.Instant.ofEpochMilli;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.webAppContextSetup;

@RunWith(SpringRunner.class)
@ActiveProfiles("test")
@ContextConfiguration(classes = {HistoryController.class})
@WebMvcTest(controllers = HistoryController.class, secure = false)
public class HistoryControllerTest {

    @Autowired
    private WebApplicationContext context;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private HistoryService historyService;

    private MockMvc mockMvc;

    @Before
    public void setup() {
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        mockMvc = webAppContextSetup(context).build();
    }

    @Test
    public void shouldConfigureHistoryController() {
        assertThat(context.containsBean("historyController"), is(true));
    }

    @Test
    public void shouldReturnHistoryAsJson() throws Exception {
        when(historyService.getHistory(any(String.class), any(String.class))).thenReturn(someHistory());
        mockMvc
                .perform(get("/internal/history/foo/4711"))
                .andDo(print())
                .andExpect(status().isOk())
                .andExpect(content().string(is("{\"history\":{\"entityId\":\"4711\",\"entries\":[{\"messageKey\":\"4711\",\"messagePayload\":{\"price\":45},\"arrivalTimestamp\":\"1970-01-01T00:00:00.005Z\",\"channelName\":\"test-products\",\"diffs\":[{\"key\":\"price\",\"previousValue\":46,\"newValue\":45,\"equal\":false}]},{\"messageKey\":\"4711\",\"messagePayload\":{\"price\":42},\"arrivalTimestamp\":\"1970-01-01T00:00:00.010Z\",\"channelName\":\"test-products\",\"diffs\":[{\"key\":\"price\",\"previousValue\":45,\"newValue\":42,\"equal\":false}]}]}}")));
    }

    public History someHistory() {
        return new History(
                "4711",
                asList(
                        new HistoryEntry(
                                message("4711", responseHeader(fromPosition("shard-1", "1"), ofEpochMilli(5)), singletonMap("price", 45)),
                                "test-products",
                                asList(
                                        new Diff("price", 46, 45))
                        ),
                        new HistoryEntry(
                                message("4711", responseHeader(fromPosition("shard-1", "1"), ofEpochMilli(10)), singletonMap("price", 42)),
                                "test-products",
                                asList(
                                        new Diff("price", 45, 42))
                        )
                ));
    }

}