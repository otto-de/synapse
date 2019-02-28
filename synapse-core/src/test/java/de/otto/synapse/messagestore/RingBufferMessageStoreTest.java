package de.otto.synapse.messagestore;

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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static de.otto.synapse.message.Header.of;
import static java.lang.String.valueOf;
import static java.util.Arrays.asList;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Tests specific for all RingBufferMessageStore implementations
 */
@RunWith(Parameterized.class)
public class RingBufferMessageStoreTest {

    @Parameters
    public static Iterable<? extends Supplier<MessageStore>> messageStores() {
        return asList(
                () -> new InMemoryRingBufferMessageStore("test")
        );
    }

    @Parameter
    public Supplier<MessageStore> messageStoreBuilder;

    @Test
    public void shouldKeepNoMoreThanCapacityIndicates() {
        final MessageStore messageStore = messageStoreBuilder.get();
        for (int i=0; i<101; ++i) {
            messageStore.add(MessageStoreEntry.of("", TextMessage.of(valueOf(i), "some payload")));
        }
        assertThat(messageStore.size(), is(100));
    }

    @Test
    public void shouldRemoveOldestIfCapacityIsReached() {
        final MessageStore messageStore = messageStoreBuilder.get();
        for (int i=0; i<102; ++i) {
            messageStore.add(MessageStoreEntry.of("", TextMessage.of(valueOf(i), "some payload")));
        }
        final AtomicInteger expectedKey = new AtomicInteger(2);
        messageStore.stream().forEach(entry -> {
            assertThat(entry.getTextMessage().getKey(), is(Key.of(valueOf(expectedKey.get()))));
            assertThat(entry.getTextMessage().getPayload(), is("some payload"));
            expectedKey.incrementAndGet();
        });
    }

    @SuppressWarnings("Duplicates")
    @Test
    public void shouldCalculateMultiShardedChannelPosition() {
        final MessageStore messageStore = messageStoreBuilder.get();
        final ExecutorService executorService = newFixedThreadPool(10);
        final CompletableFuture[] completion = new CompletableFuture[5];

        for (int i=0; i<5; ++i) {
            for (int shard = 0; shard < 5; ++shard) {
                final String shardId = valueOf(shard);
                completion[shard] = CompletableFuture.runAsync(() -> {
                    for (int pos = 0; pos < 1000; ++pos) {
                        messageStore.add(MessageStoreEntry.of("", TextMessage.of(valueOf(pos), of(fromPosition("shard-" + shardId, valueOf(pos))), "some payload")));
                        assertThat(messageStore.getLatestChannelPosition("").shard("shard-" + shardId).startFrom(), is(StartFrom.POSITION));
                        assertThat(messageStore.getLatestChannelPosition("").shard("shard-" + shardId).position(), is(valueOf(pos)));
                    }

                }, executorService);
            }
            allOf(completion).join();
        }

        ChannelPosition channelPosition = messageStore.getLatestChannelPosition("");
        assertThat(channelPosition.shards(), containsInAnyOrder("shard-0", "shard-1", "shard-2", "shard-3", "shard-4"));
        assertThat(channelPosition.shard("shard-0").position(), is("999"));
        assertThat(channelPosition.shard("shard-1").position(), is("999"));
        assertThat(channelPosition.shard("shard-2").position(), is("999"));
        assertThat(channelPosition.shard("shard-3").position(), is("999"));
        assertThat(channelPosition.shard("shard-4").position(), is("999"));
        assertThat(messageStore.size(), is(100));
    }

    @SuppressWarnings("Duplicates")
    @Test
    public void shouldCalculateChannelPosition() {
        final MessageStore messageStore = messageStoreBuilder.get();
        for (int i=0; i<5; ++i) {
            for (int pos = 0; pos < 10; ++pos) {
                messageStore.add(MessageStoreEntry.of("", TextMessage.of(valueOf(pos), of(fromPosition("some-shard", valueOf(pos))), "some payload")));
                assertThat(messageStore.getLatestChannelPosition("").shard("some-shard").startFrom(), is(StartFrom.POSITION));
                assertThat(messageStore.getLatestChannelPosition("").shard("some-shard").position(), is(valueOf(pos)));
            }
        }
        assertThat(messageStore.getLatestChannelPosition("").shards(), contains("some-shard"));
        assertThat(messageStore.getLatestChannelPosition("").shard("some-shard").position(), is("9"));
        assertThat(messageStore.size(), is(50));
    }
}