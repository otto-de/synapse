package de.otto.synapse.messagestore;

import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static de.otto.synapse.channel.ChannelPosition.fromHorizon;
import static de.otto.synapse.channel.ChannelPosition.shardPosition;
import static de.otto.synapse.message.Header.responseHeader;
import static de.otto.synapse.message.Message.message;
import static java.lang.String.valueOf;
import static java.time.Instant.now;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

public class InMemoryMessageStoreTest {

    @Test
    public void shouldAddMessagesWithoutHeaders() {
        final InMemoryMessageStore messageStore = new InMemoryMessageStore();
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

    @Test
    public void shouldCalculateChannelPosition() {
        final InMemoryMessageStore messageStore = new InMemoryMessageStore();
        final ExecutorService executorService = newFixedThreadPool(10);
        final CompletableFuture[] completion = new CompletableFuture[5];
        for (int shard=0; shard<5; ++shard) {
            final String shardId = valueOf(shard);
            completion[shard] = CompletableFuture.runAsync(() -> {
                for (int pos = 0; pos < 10000; ++pos) {
                    messageStore.add(message(valueOf(pos), responseHeader(shardPosition("shard-" + shardId, valueOf(pos)), now()), "some payload"));
                    assertThat(messageStore.getLatestChannelPosition().positionOf("shard-" + shardId), is(valueOf(pos)));
                }

            }, executorService);
        };
        allOf(completion).join();
        assertThat(messageStore.getLatestChannelPosition().shards(), containsInAnyOrder("shard-0", "shard-1", "shard-2", "shard-3", "shard-4"));
        assertThat(messageStore.getLatestChannelPosition().positionOf("shard-0"), is("9999"));
        assertThat(messageStore.getLatestChannelPosition().positionOf("shard-1"), is("9999"));
        assertThat(messageStore.getLatestChannelPosition().positionOf("shard-2"), is("9999"));
        assertThat(messageStore.getLatestChannelPosition().positionOf("shard-3"), is("9999"));
        assertThat(messageStore.getLatestChannelPosition().positionOf("shard-4"), is("9999"));
        assertThat(messageStore.size(), is(50000));
    }
}