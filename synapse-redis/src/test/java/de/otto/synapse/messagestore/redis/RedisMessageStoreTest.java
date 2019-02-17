package de.otto.synapse.messagestore.redis;

import de.otto.synapse.message.Message;
import de.otto.synapse.message.TextMessage;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.springframework.data.redis.core.BoundHashOperations;
import org.springframework.data.redis.core.RedisTemplate;

import static de.otto.synapse.channel.ChannelPosition.fromHorizon;
import static de.otto.synapse.messagestore.redis.RedisMessageStore.messageOf;
import static de.otto.synapse.messagestore.redis.RedisMessageStore.toRedisValue;
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
        testee = new RedisMessageStore("some channel", redisTemplate, 10, 20);
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

    @Test
    public void shouldConvertMessageToRedisValueAndViceVersa() {
        final TextMessage message = TextMessage.of("some key","{}");
        final String redisValue = toRedisValue(message);
        assertThat(redisValue, is("{\"_synapse_msg_format\":\"v2\",\"_synapse_msg_key\":{\"partitionKey\":\"some key\",\"compactionKey\":\"some key\"},\"_synapse_msg_headers\":{},\"_synapse_msg_payload\":{}}"));
        final Message<String> transformed = messageOf(redisValue);
        assertThat(transformed.getKey(), is(message.getKey()));
        assertThat(transformed.getPayload(), is(message.getPayload()));
    }

    @Test
    public void shouldConvertMessageWithNullPayloadToRedisValueAndViceVersa() {
        final TextMessage message = TextMessage.of("some key", null);
        final String redisValue = toRedisValue(message);
        assertThat(redisValue, is("{\"_synapse_msg_format\":\"v2\",\"_synapse_msg_key\":{\"partitionKey\":\"some key\",\"compactionKey\":\"some key\"},\"_synapse_msg_headers\":{},\"_synapse_msg_payload\":null}"));
        final Message<String> transformed = messageOf(redisValue);
        assertThat(transformed.getKey(), is(message.getKey()));
        assertThat(transformed.getPayload(), is(message.getPayload()));
    }

}