package de.otto.synapse.messagestore;

import de.otto.synapse.channel.StartFrom;
import de.otto.synapse.message.Key;
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
import static de.otto.synapse.message.Message.message;
import static java.lang.String.valueOf;
import static java.util.Arrays.asList;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Tests specific for all compacting MessageStore implementations
 */
@RunWith(Parameterized.class)
public class CompactingMessageStoreTest {

    @Parameters
    public static Iterable<? extends Supplier<WritableMessageStore>> messageStores() {
        return asList(
                CompactingInMemoryMessageStore::new,
                CompactingConcurrentMapMessageStore::new,
                () -> new CompactingConcurrentMapMessageStore(true, new ConcurrentHashMap<>())
        );
    }

    @Parameter
    public Supplier<WritableMessageStore> messageStoreBuilder;

    @Test
    public void shouldCompactMessagesByKey() {
        final WritableMessageStore messageStore = messageStoreBuilder.get();
        for (int i=0; i<10; ++i) {
            messageStore.add(message(Key.of(valueOf(i)), "some payload"));
        }
        for (int i=0; i<10; ++i) {
            messageStore.add(message(Key.of(valueOf(i)), "some updated payload"));
        }
        assertThat(messageStore.getLatestChannelPosition(), is(fromHorizon()));
        final AtomicInteger expectedKey = new AtomicInteger(0);
        messageStore.stream().forEach(message -> {
            assertThat(message.getKey(), is(Key.of(valueOf(expectedKey.get()))));
            assertThat(message.getPayload(), is("some updated payload"));
            expectedKey.incrementAndGet();
        });
        assertThat(messageStore.size(), is(10));
    }

    @Test
    public void shouldCalculateCompactedMultiShardedChannelPosition() {
        final WritableMessageStore messageStore = messageStoreBuilder.get();
        final ExecutorService executorService = newFixedThreadPool(10);
        final CompletableFuture[] completion = new CompletableFuture[5];

        for (int i=0; i<5; ++i) {
            for (int shard = 0; shard < 5; ++shard) {
                final Integer shardNumber = shard;
                final String entityId = valueOf(shard);
                completion[shard] = CompletableFuture.runAsync(() -> {
                    for (int pos = 0; pos < 1000; ++pos) {
                        messageStore.add(message(Key.of(entityId, entityId + ":" + pos), of(fromPosition("shard-" + shardNumber, valueOf(pos))), "some payload"));
                        assertThat(messageStore.getLatestChannelPosition().shard("shard-" + entityId).startFrom(), is(StartFrom.POSITION));
                        assertThat(messageStore.getLatestChannelPosition().shard("shard-" + entityId).position(), is(valueOf(pos)));
                    }

                }, executorService);
            }
            allOf(completion).join();
        }

        assertThat(messageStore.getLatestChannelPosition().shards(), containsInAnyOrder("shard-0", "shard-1", "shard-2", "shard-3", "shard-4"));
        assertThat(messageStore.getLatestChannelPosition().shard("shard-0").position(), is("999"));
        assertThat(messageStore.getLatestChannelPosition().shard("shard-1").position(), is("999"));
        assertThat(messageStore.getLatestChannelPosition().shard("shard-2").position(), is("999"));
        assertThat(messageStore.getLatestChannelPosition().shard("shard-3").position(), is("999"));
        assertThat(messageStore.getLatestChannelPosition().shard("shard-4").position(), is("999"));
        assertThat(messageStore.size(), is(5000));
    }

    @Test
    public void shouldCalculateCompactedChannelPosition() {
        final WritableMessageStore messageStore = messageStoreBuilder.get();
        for (int i=0; i<5; ++i) {
            for (int pos = 0; pos < 10000; ++pos) {
                messageStore.add(message(Key.of(valueOf(pos)), of(fromPosition("some-shard", valueOf(pos))), "some payload"));
                assertThat(messageStore.getLatestChannelPosition().shard("some-shard").startFrom(), is(StartFrom.POSITION));
                assertThat(messageStore.getLatestChannelPosition().shard("some-shard").position(), is(valueOf(pos)));
            }
        }
        assertThat(messageStore.getLatestChannelPosition().shards(), contains("some-shard"));
        assertThat(messageStore.getLatestChannelPosition().shard("some-shard").position(), is("9999"));
        assertThat(messageStore.size(), is(10000));
    }

    @Test
    public void shouldRemoveMessagesWithoutChannelPositionWithNullPayload() {
        final WritableMessageStore messageStore = messageStoreBuilder.get();
        for (int i=0; i<10; ++i) {
            messageStore.add(message(Key.of(valueOf(i)), "some payload"));
        }
        for (int i=0; i<10; ++i) {
            messageStore.add(message(Key.of(valueOf(i)), null));
        }
        assertThat(messageStore.size(), is(0));
        assertThat(messageStore.getLatestChannelPosition(), is(fromHorizon()));
    }

    @Test
    public void shouldRemoveMessagesWithNullPayload() {
        final WritableMessageStore messageStore = messageStoreBuilder.get();
        for (int i=0; i<10; ++i) {
            messageStore.add(message(
                    Key.of(valueOf(i)),
                    of(fromPosition("foo", valueOf(i))),
                    "some foo payload"));
            messageStore.add(message(
                    Key.of(valueOf(i)),
                    of(fromPosition("bar", valueOf(i))),
                    "some bar payload"));
        }
        for (int i=0; i<10; ++i) {
            messageStore.add(message(Key.of(valueOf(i)), of(fromPosition("foo", valueOf(20 + i))), null));
            messageStore.add(message(Key.of(valueOf(i)), of(fromPosition("bar", valueOf(42 + i))), null));
        }
        assertThat(messageStore.getLatestChannelPosition(), is(channelPosition(
                fromPosition("foo", "29"),
                fromPosition("bar", "51")))
        );
        assertThat(messageStore.size(), is(0));
        assertThat(messageStore.stream().count(), is(0L));
    }

}