package de.otto.synapse.messagestore.redis;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.springframework.data.redis.core.BoundHashOperations;
import org.springframework.data.redis.core.RedisTemplate;

import static de.otto.synapse.channel.ChannelPosition.fromHorizon;
import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class RedisMessageStoreTest {

    @Mock
    private RedisTemplate<String, String> redisTemplate;
    @Mock
    BoundHashOperations<String, Object, Object> ops;
    private RedisMessageStore testee;

    @Before
    public void before() {
        initMocks(this);
        when(redisTemplate.boundHashOps(anyString())).thenReturn(ops);
        testee = new RedisMessageStore("some channel", 10, 20, redisTemplate);
    }

    @Test
    public void shouldReturnChannelName() {
        assertThat(testee.getChannelName(), is("some channel"));
    }

    @Test
    public void shouldReturnFromHorizonFromEmptyMessageStore() {
        when(ops.entries()).thenReturn(emptyMap());
        assertThat(testee.getLatestChannelPosition(), is(fromHorizon()));
    }


}