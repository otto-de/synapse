package de.otto.synapse.messagestore.redis;

import de.otto.synapse.channel.ShardPosition;
import de.otto.synapse.channel.StartFrom;
import de.otto.synapse.message.Header;
import de.otto.synapse.messagestore.WritableMessageStore;
import de.otto.synapse.testsupport.redis.EmbededRedis;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static de.otto.synapse.channel.ChannelPosition.fromHorizon;
import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static de.otto.synapse.channel.StartFrom.POSITION;
import static de.otto.synapse.message.Header.responseHeader;
import static de.otto.synapse.message.Message.message;
import static java.lang.String.valueOf;
import static java.time.Instant.now;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@RunWith(SpringRunner.class)
@EnableAutoConfiguration
@ComponentScan(
        basePackages = {"de.otto.synapse.messagestore.redis"})
@SpringBootTest(
        properties = {
                "spring.redis.server=localhost",
                "spring.redis.port=6379"
        },
        classes = {
                RedisMessageStoreIntegrationTest.class,
                EmbededRedis.class
        })

@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class RedisMessageStoreIntegrationTest {


    @Configuration
    static class TestConfiguration {
        @Bean
        public RedisMessageStore redisMessageStore(final RedisTemplate<String, String> redisTemplate) {
            return new RedisMessageStore("foo-channel", redisTemplate, 100, 1000);
        }
    }

    @Autowired
    private RedisMessageStore messageStore;

    @Before
    public void before() {
        messageStore.clear();
    }

    @SuppressWarnings("Duplicates")
    @Test
    public void shouldCalculateUncompactedChannelPositions() {
        final ExecutorService executorService = newFixedThreadPool(10);
        final CompletableFuture[] completion = new CompletableFuture[5];
        for (int shard=0; shard<5; ++shard) {
            final String shardId = valueOf(shard);
            completion[shard] = CompletableFuture.runAsync(() -> {
                for (int pos = 0; pos < 1500; ++pos) {
                    messageStore.add(message(valueOf(pos), responseHeader(fromPosition("shard-" + shardId, valueOf(pos)), now()), "some payload"));
                    assertThat(messageStore.getLatestChannelPosition().shard("shard-" + shardId).startFrom(), is(StartFrom.POSITION));
                    assertThat(messageStore.getLatestChannelPosition().shard("shard-" + shardId).position(), is(valueOf(pos)));
                }

            }, executorService);
        };
        allOf(completion).join();
        assertThat(messageStore.getLatestChannelPosition().shards(), containsInAnyOrder("shard-0", "shard-1", "shard-2", "shard-3", "shard-4"));
        assertThat(messageStore.getLatestChannelPosition().shard("shard-0").position(), is("1499"));
        assertThat(messageStore.getLatestChannelPosition().shard("shard-1").position(), is("1499"));
        assertThat(messageStore.getLatestChannelPosition().shard("shard-2").position(), is("1499"));
        assertThat(messageStore.getLatestChannelPosition().shard("shard-3").position(), is("1499"));
        assertThat(messageStore.getLatestChannelPosition().shard("shard-4").position(), is("1499"));
        assertThat(messageStore.size(), is(1000));
    }

    @SuppressWarnings("Duplicates")
    @Test
    public void shouldAddMessagesWithoutHeaders() {
        for (int i=0; i<10; ++i) {
            messageStore.add(message(valueOf(i), "some payload"));
        }
        assertThat(messageStore.getLatestChannelPosition(), is(fromHorizon()));
        final AtomicInteger expectedKey = new AtomicInteger(0);
        messageStore.stream().forEach(message -> {
            assertThat(message.getKey(), is(valueOf(expectedKey.get())));
            expectedKey.incrementAndGet();
        });
        assertThat(messageStore.size(), is(10));
    }

    @SuppressWarnings("Duplicates")
    @Test
    public void shouldKeepInsertionOrderOfMessages() {
        final ExecutorService executorService = newFixedThreadPool(10);
        final CompletableFuture[] completion = new CompletableFuture[5];
        for (int shard=0; shard<5; ++shard) {
            final String shardId = valueOf(shard);
            completion[shard] = CompletableFuture.runAsync(() -> {
                for (int pos = 0; pos < 1500; ++pos) {
                    messageStore.add(message(valueOf(pos), responseHeader(fromPosition("shard-" + shardId, valueOf(pos)), now()), "some payload"));
                    assertThat(messageStore.getLatestChannelPosition().shard("shard-" + shardId).startFrom(), is(POSITION));
                    assertThat(messageStore.getLatestChannelPosition().shard("shard-" + shardId).position(), is(valueOf(pos)));
                }

            }, executorService);
        };
        allOf(completion).join();
        final Map<String, Integer> lastPositions = new HashMap<>();
        messageStore.stream().forEach(message -> {
            final Header header = message.getHeader();
            if (header.getShardPosition().isPresent()) {
                final ShardPosition shard = header.getShardPosition().get();
                final Integer pos = Integer.valueOf(shard.position());
                lastPositions.putIfAbsent(shard.shardName(), pos);
                assertThat(lastPositions.get(shard.shardName()), is(lessThanOrEqualTo(pos)));
            }
        });
        assertThat(messageStore.size(), isOneOf(10000, 1000));
    }
}