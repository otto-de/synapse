package de.otto.synapse.messagestore.redis;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.springframework.data.redis.core.BoundHashOperations;
import org.springframework.data.redis.core.BoundSetOperations;
import org.springframework.data.redis.core.RedisTemplate;

import static de.otto.synapse.channel.ChannelPosition.channelPosition;
import static de.otto.synapse.channel.ChannelPosition.fromHorizon;
import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class RedisMessageStoreTest {

    @Mock
    private RedisTemplate<String, String> redisTemplate;
    @Mock
    BoundHashOperations<String, Object, Object> hashOperations;
    @Mock
    BoundSetOperations<String, String> setOperations;
    private RedisMessageStore testee;

    @Before
    public void before() {
        initMocks(this);
        when(redisTemplate.boundHashOps(anyString())).thenReturn(hashOperations);
        when(redisTemplate.boundSetOps(anyString())).thenReturn(setOperations);
        testee = new RedisMessageStore("Test Store", 10, 20, redisTemplate);
    }

    @Test
    public void shouldReturnMessageStoreName() {
        assertThat(testee.getName(), is("Test Store"));
    }

    @Test
    public void shouldReturnChannelNames() {
        when(setOperations.members()).thenReturn(ImmutableSet.of("foo", "bar"));
        assertThat(testee.getChannelNames(), containsInAnyOrder("foo", "bar"));
    }

    @Test
    public void shouldReturnFromHorizonFromEmptyMessageStore() {
        when(setOperations.members()).thenReturn(emptySet());
        assertThat(testee.getLatestChannelPosition(), is(fromHorizon()));
    }

    @Test
    public void shouldReturnPosition() {
        when(hashOperations.entries()).thenReturn(ImmutableMap.of("foo-1", "42", "foo-2", "0815"));
        assertThat(testee.getLatestChannelPosition("foo"), is(channelPosition(
                fromPosition("foo-1", "42"),
                fromPosition("foo-2", "0815"))
        ));
    }


}