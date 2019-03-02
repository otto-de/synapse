package de.otto.synapse.messagestore;

import com.google.common.collect.ImmutableList;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.StartFrom;
import de.otto.synapse.message.Key;
import de.otto.synapse.message.TextMessage;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static de.otto.synapse.channel.ChannelPosition.channelPosition;
import static de.otto.synapse.channel.ChannelPosition.fromHorizon;
import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static de.otto.synapse.message.Header.of;
import static java.lang.String.valueOf;
import static java.util.Arrays.asList;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Tests specific for all compacting MessageStore implementations
 */
@RunWith(Parameterized.class)
public class CompactingMessageStoreTest {

    @Parameters
    public static Iterable<? extends Supplier<MessageStore>> messageStores() {
        return asList(
                () -> new CompactingInMemoryMessageStore("test", true),
                () -> new CompactingConcurrentMapMessageStore("test", true, new ConcurrentHashMap<>()),
                () -> new CompactingConcurrentMapMessageStore("test", true, new ConcurrentHashMap<>())
        );
    }

    @Parameter
    public Supplier<MessageStore> messageStoreBuilder;

    @Test
    public void shouldCompactMessagesFromSameChannelByCompactionKey() {
        final MessageStore messageStore = messageStoreBuilder.get();
        for (int i=0; i<10; ++i) {
            messageStore.add(MessageStoreEntry.of(
                    "some-channel",
                    TextMessage.of(Key.of("alwaysTheSamePartition", valueOf(i)), "some payload"))
            );
        }
        for (int i=0; i<10; ++i) {
            messageStore.add(MessageStoreEntry.of(
                    "some-channel",
                    TextMessage.of(Key.of(valueOf(i)), "some updated payload")));
        }
        assertThat(messageStore.getLatestChannelPosition("some-channel"), is(fromHorizon()));
        final AtomicInteger expectedKey = new AtomicInteger(0);
        messageStore.stream().map(MessageStoreEntry::getTextMessage).forEach(message -> {
            assertThat(message.getKey(), is(Key.of(valueOf(expectedKey.get()))));
            assertThat(message.getPayload(), is("some updated payload"));
            expectedKey.incrementAndGet();
        });
        assertThat(messageStore.size(), is(10));
    }

    @Test
    public void shouldNotCompactMessagesFromDifferentChannel() {
        final MessageStore messageStore = messageStoreBuilder.get();
        for (int i=0; i<10; ++i) {
            messageStore.add(MessageStoreEntry.of(
                    "some-channel",
                    TextMessage.of(Key.of("alwaysTheSamePartition", valueOf(i)), "some payload"))
            );
        }
        for (int i=0; i<10; ++i) {
            messageStore.add(MessageStoreEntry.of(
                    "other-channel",
                    TextMessage.of(Key.of(valueOf(i)), "some updated payload")));
        }

        assertThat(messageStore.size(), is(20));
        assertThat(messageStore.stream().filter(entry -> entry.getChannelName().equals("some-channel")).count(), is(10L));
        assertThat(messageStore.stream().filter(entry -> entry.getChannelName().equals("other-channel")).count(), is(10L));
    }

    @Test
    public void shouldCalculateCompactedMultiShardedChannelPosition() {
        final MessageStore messageStore = messageStoreBuilder.get();
        final ExecutorService executorService = newFixedThreadPool(10);
        final CompletableFuture[] completion = new CompletableFuture[5];

        for (int i=0; i<5; ++i) {
            for (int shard = 0; shard < 5; ++shard) {
                final Integer shardNumber = shard;
                final String entityId = valueOf(shard);
                completion[shard] = CompletableFuture.runAsync(() -> {
                    for (int pos = 0; pos < 1000; ++pos) {
                        messageStore.add(MessageStoreEntry.of("some-channel", TextMessage.of(Key.of(entityId, entityId + ":" + pos), of(fromPosition("shard-" + shardNumber, valueOf(pos))), "some payload")));
                        assertThat(messageStore.getLatestChannelPosition("some-channel").shard("shard-" + entityId).startFrom(), is(StartFrom.POSITION));
                        assertThat(messageStore.getLatestChannelPosition("some-channel").shard("shard-" + entityId).position(), is(valueOf(pos)));
                    }

                }, executorService);
            }
            allOf(completion).join();
        }

        final ChannelPosition channelPosition = messageStore.getLatestChannelPosition("some-channel");
        assertThat(channelPosition.shards(), containsInAnyOrder("shard-0", "shard-1", "shard-2", "shard-3", "shard-4"));
        assertThat(channelPosition.shard("shard-0").position(), is("999"));
        assertThat(channelPosition.shard("shard-1").position(), is("999"));
        assertThat(channelPosition.shard("shard-2").position(), is("999"));
        assertThat(channelPosition.shard("shard-3").position(), is("999"));
        assertThat(channelPosition.shard("shard-4").position(), is("999"));
        assertThat(messageStore.size(), is(5000));
    }

    @Test
    public void shouldCalculateCompactedMultiShardedChannelPositionForMultipleChannels() {
        final MessageStore messageStore = messageStoreBuilder.get();
        final ExecutorService executorService = newFixedThreadPool(10);
        final CompletableFuture[] completion = new CompletableFuture[10];

        for (int i=0; i<5; ++i) {
            for (int shard = 0; shard < 5; ++shard) {
                final Integer shardNumber = shard;
                final String entityId = valueOf(shard);
                completion[shard] = CompletableFuture.runAsync(() -> {
                    for (int pos = 0; pos < 1000; ++pos) {
                        messageStore.add(MessageStoreEntry.of("some-channel", TextMessage.of(Key.of(entityId, entityId + ":" + pos), of(fromPosition("shard-" + shardNumber, valueOf(pos))), "some payload")));
                        assertThat(messageStore.getLatestChannelPosition("some-channel").shard("shard-" + entityId).startFrom(), is(StartFrom.POSITION));
                        assertThat(messageStore.getLatestChannelPosition("some-channel").shard("shard-" + entityId).position(), is(valueOf(pos)));
                    }

                }, executorService);
                completion[5+shard] = CompletableFuture.runAsync(() -> {
                    for (int pos = 0; pos < 1000; ++pos) {
                        messageStore.add(MessageStoreEntry.of("other-channel", TextMessage.of(Key.of(entityId, entityId + ":" + pos), of(fromPosition("shard-" + shardNumber, valueOf(pos))), "some payload")));
                        assertThat(messageStore.getLatestChannelPosition("other-channel").shard("shard-" + entityId).startFrom(), is(StartFrom.POSITION));
                        assertThat(messageStore.getLatestChannelPosition("other-channel").shard("shard-" + entityId).position(), is(valueOf(pos)));
                    }

                }, executorService);
            }
            allOf(completion).join();
        }

        assertThat(messageStore.size(), is(10000));
        final ChannelPosition someChannelPosition = messageStore.getLatestChannelPosition("some-channel");
        assertThat(someChannelPosition.shards(), containsInAnyOrder("shard-0", "shard-1", "shard-2", "shard-3", "shard-4"));
        assertThat(someChannelPosition.shard("shard-0").position(), is("999"));
        assertThat(someChannelPosition.shard("shard-1").position(), is("999"));
        assertThat(someChannelPosition.shard("shard-2").position(), is("999"));
        assertThat(someChannelPosition.shard("shard-3").position(), is("999"));
        assertThat(someChannelPosition.shard("shard-4").position(), is("999"));
        final ChannelPosition otherChannelPosition = messageStore.getLatestChannelPosition("other-channel");
        assertThat(otherChannelPosition.shards(), containsInAnyOrder("shard-0", "shard-1", "shard-2", "shard-3", "shard-4"));
        assertThat(otherChannelPosition.shard("shard-0").position(), is("999"));
        assertThat(otherChannelPosition.shard("shard-1").position(), is("999"));
        assertThat(otherChannelPosition.shard("shard-2").position(), is("999"));
        assertThat(otherChannelPosition.shard("shard-3").position(), is("999"));
        assertThat(otherChannelPosition.shard("shard-4").position(), is("999"));
    }

    @Test
    public void shouldCalculateCompactedChannelPosition() {
        final MessageStore messageStore = messageStoreBuilder.get();
        for (int i=0; i<5; ++i) {
            for (int pos = 0; pos < 10000; ++pos) {
                messageStore.add(MessageStoreEntry.of("some-channel", TextMessage.of(Key.of(valueOf(pos)), of(fromPosition("some-shard", valueOf(pos))), "some payload")));
                assertThat(messageStore.getLatestChannelPosition("some-channel").shard("some-shard").startFrom(), is(StartFrom.POSITION));
                assertThat(messageStore.getLatestChannelPosition("some-channel").shard("some-shard").position(), is(valueOf(pos)));
            }
        }
        assertThat(messageStore.getLatestChannelPosition("some-channel").shards(), contains("some-shard"));
        assertThat(messageStore.getLatestChannelPosition("some-channel").shard("some-shard").position(), is("9999"));
        assertThat(messageStore.size(), is(10000));
    }

    @Test
    public void shouldRemoveMessagesWithoutChannelPositionWithNullPayload() {
        final MessageStore messageStore = messageStoreBuilder.get();
        for (int i=0; i<10; ++i) {
            messageStore.add(MessageStoreEntry.of("some-channel", TextMessage.of(Key.of(valueOf(i)), "some payload")));
            messageStore.add(MessageStoreEntry.of("other-channel", TextMessage.of(Key.of(valueOf(i)), "other payload")));
        }
        for (int i=0; i<10; ++i) {
            messageStore.add(MessageStoreEntry.of("some-channel", TextMessage.of(Key.of(valueOf(i)), null)));
        }
        assertThat(messageStore.size(), is(10));
        assertThat(messageStore.stream().map(MessageStoreEntry::getChannelName).distinct().collect(toList()), is(ImmutableList.of("other-channel")));
    }

    @Test
    public void shouldRemoveMessagesWithNullPayload() {
        final MessageStore messageStore = messageStoreBuilder.get();
        for (int i=0; i<10; ++i) {
            messageStore.add(MessageStoreEntry.of("some-channel", TextMessage.of(
                    Key.of(valueOf(i)),
                    of(fromPosition("foo", valueOf(i))),
                    "some foo payload")));
            messageStore.add(MessageStoreEntry.of("some-channel", TextMessage.of(
                    Key.of(valueOf(i)),
                    of(fromPosition("bar", valueOf(i))),
                    "some bar payload")));
        }
        for (int i=0; i<10; ++i) {
            messageStore.add(MessageStoreEntry.of(
                    "some-channel",
                    TextMessage.of(Key.of(valueOf(i)), of(fromPosition("foo", valueOf(20 + i))), null)));
            messageStore.add(MessageStoreEntry.of(
                    "some-channel",
                    TextMessage.of(Key.of(valueOf(i)), of(fromPosition("bar", valueOf(42 + i))), null)));
        }
        assertThat(messageStore.getLatestChannelPosition("some-channel"), is(channelPosition(
                fromPosition("foo", "29"),
                fromPosition("bar", "51")))
        );
        assertThat(messageStore.size(), is(0));
        assertThat(messageStore.stream().count(), is(0L));
    }

}