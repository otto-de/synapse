package de.otto.synapse.messagestore.redis;

import com.google.common.collect.Range;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.ShardPosition;
import de.otto.synapse.channel.StartFrom;
import de.otto.synapse.message.Header;
import de.otto.synapse.message.Key;
import de.otto.synapse.message.Message;
import de.otto.synapse.message.TextMessage;
import de.otto.synapse.messagestore.MessageStoreEntry;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static de.otto.synapse.channel.ChannelPosition.channelPosition;
import static de.otto.synapse.channel.ChannelPosition.fromHorizon;
import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static de.otto.synapse.channel.StartFrom.POSITION;
import static de.otto.synapse.message.Header.of;
import static java.lang.String.valueOf;
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
                "spring.redis.port=6079"
        },
        classes = {
                RedisRingBufferMessageStoreIntegrationTest.class,
                EmbededRedis.class
        })

@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class RedisRingBufferMessageStoreIntegrationTest {


    @Configuration
    static class TestConfiguration {
        @Bean
        public RedisRingBufferMessageStore redisMessageStore(final RedisTemplate<String, String> redisTemplate) {
            return new RedisRingBufferMessageStore("Test Store", 100, 1000, redisTemplate);
        }
    }

    @Autowired
    private RedisRingBufferMessageStore messageStore;

    @Before
    public void before() {
        messageStore.clear();
    }

    @Test
    public void shouldProvideChannelPositionsForMultipleChannelsAndShards() {
        messageStore.add(MessageStoreEntry.of(
                "first.shouldProvideChannelPositionsForMultipleChannelsAndShards",
                TextMessage.of("1", of(fromPosition("shard-1", "1")), "")));
        messageStore.add(MessageStoreEntry.of(
                "second.shouldProvideChannelPositionsForMultipleChannelsAndShards",
                TextMessage.of("2", of(fromPosition("shard-1", "42")), "")));
        messageStore.add(MessageStoreEntry.of(
                "first.shouldProvideChannelPositionsForMultipleChannelsAndShards",
                TextMessage.of("3", of(fromPosition("shard-2", "1")), "")));
        messageStore.add(MessageStoreEntry.of(
                "first.shouldProvideChannelPositionsForMultipleChannelsAndShards",
                TextMessage.of("4", of(fromPosition("shard-2", "2")), "")));
        assertThat(messageStore.getLatestChannelPosition("first.shouldProvideChannelPositionsForMultipleChannelsAndShards"), is(channelPosition(
                ShardPosition.fromPosition("shard-1", "1"),
                ShardPosition.fromPosition("shard-2", "2")
        )));
        assertThat(messageStore.getLatestChannelPosition("second.shouldProvideChannelPositionsForMultipleChannelsAndShards"), is(channelPosition(
                ShardPosition.fromPosition("shard-1", "42")
        )));
        assertThat(messageStore.getLatestChannelPosition("third"), is(ChannelPosition.fromHorizon()));
    }

    @SuppressWarnings("Duplicates")
    @Test
    public void shouldReturnChannelNames() {
        messageStore.add(MessageStoreEntry.of("one", TextMessage.of("1", "1")));
        messageStore.add(MessageStoreEntry.of("two", TextMessage.of("2", "2")));
        messageStore.add(MessageStoreEntry.of("one", TextMessage.of("3", "3")));

        final List<String> channelNames = messageStore
                .streamAll()
                .map(MessageStoreEntry::getChannelName)
                .collect(Collectors.toList());
        assertThat(channelNames, contains("one", "two", "one"));
    }

    @SuppressWarnings("Duplicates")
    @Test
    public void shouldStreamAllMessages() {
        messageStore.add(MessageStoreEntry.of("one", TextMessage.of("1", "1")));
        messageStore.add(MessageStoreEntry.of("two", TextMessage.of("2", "2")));
        messageStore.add(MessageStoreEntry.of("one", TextMessage.of("3", "3")));

        final List<String> messageKeys = messageStore
                .streamAll()
                .map(MessageStoreEntry::getTextMessage)
                .map(TextMessage::getKey)
                .map(Key::partitionKey)
                .collect(Collectors.toList());
        assertThat(messageKeys, contains("1", "2", "3"));
        final List<String> channelNames = messageStore
                .streamAll()
                .map(MessageStoreEntry::getChannelName)
                .collect(Collectors.toList());
        assertThat(messageKeys, contains("1", "2", "3"));
        assertThat(channelNames, contains("one", "two", "one"));
    }

    @SuppressWarnings("Duplicates")
    @Test
    public void shouldCalculateUncompactedChannelPositionsForSingleChannel() {
        final ExecutorService executorService = newFixedThreadPool(10);
        final CompletableFuture[] completion = new CompletableFuture[5];
        for (int shard=0; shard<5; ++shard) {
            final String shardId = valueOf(shard);
            completion[shard] = CompletableFuture.runAsync(() -> {
                for (int pos = 0; pos < 100; ++pos) {
                    messageStore.add(MessageStoreEntry.of("", TextMessage.of(Key.of(valueOf(pos)), of(fromPosition("shard-" + shardId, valueOf(pos))), "some payload")));
                    assertThat(messageStore.getLatestChannelPosition("").shard("shard-" + shardId).startFrom(), is(StartFrom.POSITION));
                    assertThat(messageStore.getLatestChannelPosition("").shard("shard-" + shardId).position(), is(valueOf(pos)));
                }

            }, executorService);
        }
        allOf(completion).join();
        ChannelPosition position = messageStore.getLatestChannelPosition("");
        assertThat(position.shards(), containsInAnyOrder("shard-0", "shard-1", "shard-2", "shard-3", "shard-4"));
        assertThat(position.shard("shard-0").position(), is("99"));
        assertThat(position.shard("shard-1").position(), is("99"));
        assertThat(position.shard("shard-2").position(), is("99"));
        assertThat(position.shard("shard-3").position(), is("99"));
        assertThat(position.shard("shard-4").position(), is("99"));
        assertThat(messageStore.size(), is(500));
    }

    @SuppressWarnings("Duplicates")
    @Test
    public void shouldCalculateUncompactedChannelPositionsForMultipleChannels() {
        final ExecutorService executorService = newFixedThreadPool(10);
        final CompletableFuture[] completion = new CompletableFuture[10];
        for (int shard = 0; shard<5; ++shard) {
            final String shardId = valueOf(shard);
            completion[shard] = CompletableFuture.runAsync(() -> {
                for (int pos = 0; pos < 100; ++pos) {
                    messageStore.add(MessageStoreEntry.of("first", TextMessage.of(Key.of(valueOf(pos)), of(fromPosition("shard-" + shardId, valueOf(pos))), "some payload")));
                    assertThat(messageStore.getLatestChannelPosition("first").shard("shard-" + shardId).startFrom(), is(StartFrom.POSITION));
                    assertThat(messageStore.getLatestChannelPosition("first").shard("shard-" + shardId).position(), is(valueOf(pos)));
                }

            }, executorService);
            completion[5+shard] = CompletableFuture.runAsync(() -> {
                for (int pos = 0; pos < 100; ++pos) {
                    messageStore.add(MessageStoreEntry.of("second", TextMessage.of(Key.of(valueOf(pos)), of(fromPosition("shard-" + shardId, valueOf(pos))), "some payload")));
                    assertThat(messageStore.getLatestChannelPosition("second").shard("shard-" + shardId).startFrom(), is(StartFrom.POSITION));
                    assertThat(messageStore.getLatestChannelPosition("second").shard("shard-" + shardId).position(), is(valueOf(pos)));
                }

            }, executorService);
        }
        allOf(completion).join();
        ChannelPosition firstPos = messageStore.getLatestChannelPosition("first");
        assertThat(firstPos.shards(), containsInAnyOrder("shard-0", "shard-1", "shard-2", "shard-3", "shard-4"));
        assertThat(firstPos.shard("shard-0").position(), is("99"));
        assertThat(firstPos.shard("shard-1").position(), is("99"));
        assertThat(firstPos.shard("shard-2").position(), is("99"));
        assertThat(firstPos.shard("shard-3").position(), is("99"));
        assertThat(firstPos.shard("shard-4").position(), is("99"));
        ChannelPosition secondPos = messageStore.getLatestChannelPosition("second");
        assertThat(secondPos.shards(), containsInAnyOrder("shard-0", "shard-1", "shard-2", "shard-3", "shard-4"));
        assertThat(secondPos.shard("shard-0").position(), is("99"));
        assertThat(secondPos.shard("shard-1").position(), is("99"));
        assertThat(secondPos.shard("shard-2").position(), is("99"));
        assertThat(secondPos.shard("shard-3").position(), is("99"));
        assertThat(secondPos.shard("shard-4").position(), is("99"));
        assertThat(messageStore.size(), is(1000));
    }

    @Test
    public void shouldKeepChannelName() {
        messageStore.add(MessageStoreEntry.of("first", TextMessage.of("1", "1")));
        messageStore.add(MessageStoreEntry.of("second", TextMessage.of("1", "2")));
        assertThat(messageStore.streamAll().map(MessageStoreEntry::getChannelName).collect(Collectors.toList()), contains("first", "second"));
    }

    @SuppressWarnings("Duplicates")
    @Test
    public void shouldAddMessagesWithoutHeaders() {
        for (int i=0; i<10; ++i) {
            messageStore.add(MessageStoreEntry.of("test", TextMessage.of(valueOf(i), "some payload")));
        }
        assertThat(messageStore.getLatestChannelPosition("test"), is(fromHorizon()));
        final AtomicInteger expectedKey = new AtomicInteger(0);
        messageStore.streamAll().map(MessageStoreEntry::getTextMessage).forEach(message -> {
            assertThat(message.getKey(), is(Key.of(valueOf(expectedKey.get()))));
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
                    messageStore.add(MessageStoreEntry.of("", TextMessage.of(valueOf(pos), of(fromPosition("shard-" + shardId, valueOf(pos))), "some payload")));
                    assertThat(messageStore.getLatestChannelPosition().shard("shard-" + shardId).startFrom(), is(POSITION));
                    assertThat(messageStore.getLatestChannelPosition().shard("shard-" + shardId).position(), is(valueOf(pos)));
                }

            }, executorService);
        }
        allOf(completion).join();
        final Map<String, Integer> lastPositions = new HashMap<>();
        messageStore.streamAll().map(MessageStoreEntry::getTextMessage).forEach(message -> {
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

    @Test
    public void shouldStreamLotsOfMessages() {
        for (int i = 0; i < 200; i++) {
            messageStore.add(MessageStoreEntry.of("foo-channel", TextMessage.of("" + i, null)));
        }
        final List<Integer> keys = messageStore.streamAll().map(MessageStoreEntry::getTextMessage).map(Message::getKey).map(Key::partitionKey).map(Integer::valueOf).collect(Collectors.toList());
        assertThat(keys, hasSize(200));
        assertThat(Range.closed(0, 199).containsAll(keys), is(true));
    }
}