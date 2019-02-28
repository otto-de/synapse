package de.otto.synapse.messagestore.redis;

import com.google.common.collect.ImmutableMap;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.ShardPosition;
import de.otto.synapse.channel.StartFrom;
import de.otto.synapse.message.Header;
import de.otto.synapse.message.Key;
import de.otto.synapse.message.Message;
import de.otto.synapse.message.TextMessage;
import de.otto.synapse.messagestore.Index;
import de.otto.synapse.messagestore.Indexers;
import de.otto.synapse.messagestore.MessageStoreEntry;
import de.otto.synapse.testsupport.redis.EmbededRedis;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static de.otto.synapse.channel.ChannelPosition.channelPosition;
import static de.otto.synapse.channel.ChannelPosition.fromHorizon;
import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static de.otto.synapse.channel.StartFrom.POSITION;
import static de.otto.synapse.message.Header.of;
import static de.otto.synapse.messagestore.Index.*;
import static de.otto.synapse.messagestore.Indexers.composite;
import static de.otto.synapse.messagestore.Indexers.partitionKeyIndexer;
import static de.otto.synapse.messagestore.MessageStoreEntry.of;
import static java.lang.String.valueOf;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.slf4j.LoggerFactory.getLogger;

@RunWith(SpringRunner.class)
@EnableAutoConfiguration
@ComponentScan(
        basePackages = {"de.otto.synapse.messagestore.redis"})
@SpringBootTest(
        properties = {
                "spring.redis.server=localhost",
                "spring.redis.port=6080"
        },
        classes = {
                RedisIndexedMessageStoreIntegrationTest.class,
                EmbededRedis.class
        })

@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class RedisIndexedMessageStoreIntegrationTest {

    private static final Logger LOG = getLogger(RedisIndexedMessageStoreIntegrationTest.class);

    @Configuration
    static class TestConfiguration {
        @Bean
        public RedisIndexedMessageStore redisIndexedMessageStore(final RedisTemplate<String, String> redisTemplate) {
            return new RedisIndexedMessageStore(
                    "Test Store",
                    100,
                    10000,
                    7*24*60*60*1000,
                    composite(
                            partitionKeyIndexer(),
                            Indexers.originIndexer("RedisIndexedMessageStoreIntegrationTest"),
                            Indexers.serviceInstanceIndexer("test@localhost")
                    ),
                    redisTemplate);
        }
    }

    @Autowired
    private RedisIndexedMessageStore messageStore;

    @Before
    public void before() {
        messageStore.clear();
    }

    @Test
    public void shouldProvideChannelPositionsForMultipleChannelsAndShards() {
        messageStore.add(of(
                "first.shouldProvideChannelPositionsForMultipleChannelsAndShards",
                TextMessage.of("1", of(fromPosition("shard-1", "1")), "")));
        messageStore.add(of(
                "second.shouldProvideChannelPositionsForMultipleChannelsAndShards",
                TextMessage.of("2", of(fromPosition("shard-1", "42")), "")));
        messageStore.add(of(
                "first.shouldProvideChannelPositionsForMultipleChannelsAndShards",
                TextMessage.of("3", of(fromPosition("shard-2", "1")), "")));
        messageStore.add(of(
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
        messageStore.add(of("one", TextMessage.of("1", "1")));
        messageStore.add(of("two", TextMessage.of("2", "2")));
        messageStore.add(of("one", TextMessage.of("3", "3")));

        final Set<String> channelNames = messageStore.getChannelNames();
        assertThat(channelNames, containsInAnyOrder("one", "two"));
    }

    @SuppressWarnings("Duplicates")
    @Test
    public void shouldStreamAllMessages() {
        messageStore.add(of("one", TextMessage.of("1", "1")));
        messageStore.add(of("two", TextMessage.of("2", "2")));
        messageStore.add(of("one", TextMessage.of("3", "3")));

        final List<String> messageKeys = messageStore
                .stream()
                .map(MessageStoreEntry::getTextMessage)
                .map(TextMessage::getKey)
                .map(Key::partitionKey)
                .collect(Collectors.toList());
        assertThat(messageKeys, contains("1", "2", "3"));
        final List<String> channelNames = messageStore
                .stream()
                .map(MessageStoreEntry::getChannelName)
                .collect(Collectors.toList());
        assertThat(messageKeys, contains("1", "2", "3"));
        assertThat(channelNames, contains("one", "two", "one"));
    }

    @SuppressWarnings("Duplicates")
    //@Test
    public void shouldExpireMessages() throws InterruptedException {
        messageStore.add(of("one", TextMessage.of("1", "1")));

        Thread.sleep(15000);
        messageStore.add(of("two", TextMessage.of("2", "2")));
        messageStore.add(of("one", TextMessage.of("3", "3")));

        final List<String> messageKeys = messageStore
                .stream()
                .map(MessageStoreEntry::getTextMessage)
                .map(TextMessage::getKey)
                .map(Key::partitionKey)
                .collect(Collectors.toList());
        assertThat(messageKeys, contains("2", "3"));
        final List<String> channelNames = messageStore
                .stream()
                .map(MessageStoreEntry::getChannelName)
                .collect(Collectors.toList());
        assertThat(messageKeys, contains("2", "3"));
        assertThat(channelNames, contains("two", "one"));
    }

    @SuppressWarnings("Duplicates")
    @Test
    public void shouldStreamAllMessagesForIndex() {
        final String entityOne = UUID.randomUUID().toString();
        final String entityTwo = UUID.randomUUID().toString();
        messageStore.add(of("one", TextMessage.of(entityOne, "1")));
        messageStore.add(of("two", TextMessage.of(entityTwo, "2")));
        messageStore.add(of("one", TextMessage.of(entityTwo, "3")));
        messageStore.add(of("one", TextMessage.of(entityOne, "4")));
        messageStore.add(of("two", TextMessage.of(entityOne, "5")));
        messageStore.add(of("one", TextMessage.of(entityOne, "6")));

        final List<String> payloads = messageStore
                .stream(PARTITION_KEY, entityOne)
                .map(MessageStoreEntry::getTextMessage)
                .map(TextMessage::getPayload)
                .collect(Collectors.toList());
        assertThat(payloads, contains("1", "4", "5", "6"));
    }

    @SuppressWarnings("Duplicates")
    @Test
    public void shouldReturnFilterValuesFromStream() {
        final String partitionKey = UUID.randomUUID().toString();

        messageStore.add(of("one", TextMessage.of(partitionKey, "1")));
        messageStore
                .stream(Index.ORIGIN, "Snapshot")
                .filter(entry -> entry.getFilterValues().getOrDefault(Index.SERVICE_INSTANCE.name(), "").startsWith("my-service"))
                .forEach(System.out::println);

        final MessageStoreEntry entry = messageStore
                .stream()
                .findFirst()
                .get();
        assertThat(entry.getFilterValues(), is(ImmutableMap.of(
                PARTITION_KEY, partitionKey,
                ORIGIN, "RedisIndexedMessageStoreIntegrationTest",
                SERVICE_INSTANCE, "test@localhost"
        )));
    }

    @SuppressWarnings("Duplicates")
    @Test
    public void shouldStreamAllMessagesForMultipleIndexes() {
        final String entityOne = UUID.randomUUID().toString();
        final String entityTwo = UUID.randomUUID().toString();
        messageStore.add(of("one", TextMessage.of(entityOne, "1")));
        messageStore.add(of("two", TextMessage.of(entityTwo, "2")));
        messageStore.add(of("one", TextMessage.of(entityTwo, "3")));
        messageStore.add(of("one", TextMessage.of(entityOne, "4")));
        messageStore.add(of("two", TextMessage.of(entityOne, "5")));
        messageStore.add(of("one", TextMessage.of(entityOne, "6")));

        final List<String> payloads = messageStore
                .stream(PARTITION_KEY, entityOne)
                .map(MessageStoreEntry::getTextMessage)
                .map(TextMessage::getPayload)
                .collect(Collectors.toList());
        assertThat(payloads, contains("1", "4", "5", "6"));
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
                    messageStore.add(of("", TextMessage.of(Key.of(valueOf(pos)), of(fromPosition("shard-" + shardId, valueOf(pos))), "some payload")));
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
                    messageStore.add(of("first", TextMessage.of(Key.of(valueOf(pos)), of(fromPosition("shard-" + shardId, valueOf(pos))), "some payload")));
                    assertThat(messageStore.getLatestChannelPosition("first").shard("shard-" + shardId).startFrom(), is(StartFrom.POSITION));
                    assertThat(messageStore.getLatestChannelPosition("first").shard("shard-" + shardId).position(), is(valueOf(pos)));
                }

            }, executorService);
            completion[5+shard] = CompletableFuture.runAsync(() -> {
                for (int pos = 0; pos < 100; ++pos) {
                    messageStore.add(of("second", TextMessage.of(Key.of(valueOf(pos)), of(fromPosition("shard-" + shardId, valueOf(pos))), "some payload")));
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
        messageStore.add(of("first", TextMessage.of("1", "1")));
        messageStore.add(of("second", TextMessage.of("1", "2")));
        assertThat(messageStore.stream().map(MessageStoreEntry::getChannelName).collect(Collectors.toList()), contains("first", "second"));
    }

    @SuppressWarnings("Duplicates")
    @Test
    public void shouldAddMessagesWithoutHeaders() {
        for (int i=0; i<10; ++i) {
            messageStore.add(of("test", TextMessage.of(valueOf(i), "some payload")));
        }
        assertThat(messageStore.getLatestChannelPosition("test"), is(fromHorizon()));
        final AtomicInteger expectedKey = new AtomicInteger(0);
        messageStore.stream().map(MessageStoreEntry::getTextMessage).forEach(message -> {
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
                    messageStore.add(of("", TextMessage.of(valueOf(pos), of(fromPosition("shard-" + shardId, valueOf(pos))), "some payload")));
                    assertThat(messageStore.getLatestChannelPosition().shard("shard-" + shardId).startFrom(), is(POSITION));
                    assertThat(messageStore.getLatestChannelPosition().shard("shard-" + shardId).position(), is(valueOf(pos)));
                }

            }, executorService);
        }
        allOf(completion).join();
        final Map<String, Integer> lastPositions = new HashMap<>();
        messageStore.stream().map(MessageStoreEntry::getTextMessage).forEach(message -> {
            final Header header = message.getHeader();
            if (header.getShardPosition().isPresent()) {
                final ShardPosition shard = header.getShardPosition().get();
                final Integer pos = Integer.valueOf(shard.position());
                lastPositions.putIfAbsent(shard.shardName(), pos);
                assertThat(lastPositions.get(shard.shardName()), is(lessThanOrEqualTo(pos)));
            }
        });
        assertThat(messageStore.size(), is(7500));
    }

    //@Test
    public void shouldStreamLotsOfMessages() {
        for (int j = 0; j < 100; j++) {
            for (int i = 0; i < 1000; i++) {
                messageStore.add(of("foo-channel", TextMessage.of("" + i, "" + j)));
            }
            LOG.info("1000 elements written to Redis");
        }
        AtomicInteger count = new AtomicInteger();
        final List<Integer> keys = messageStore.stream().map(entry -> {
            final int i = count.incrementAndGet();
            if (i % 1000 == 0) {
                LOG.info("{} elements read from Redis", i);
            }
            return entry.getTextMessage();
        }).map(Message::getKey).map(Key::partitionKey).map(Integer::valueOf).collect(Collectors.toList());
        messageStore.stream(PARTITION_KEY, "42").map(MessageStoreEntry::getTextMessage).forEach(System.out::println);

        assertThat(keys, hasSize(100000));
        LOG.info("Finished reading entries");
    }

}