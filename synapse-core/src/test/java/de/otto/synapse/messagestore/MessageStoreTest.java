package de.otto.synapse.messagestore;

import de.otto.synapse.channel.ShardPosition;
import de.otto.synapse.message.Header;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static de.otto.synapse.channel.ChannelPosition.fromHorizon;
import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static de.otto.synapse.channel.StartFrom.POSITION;
import static de.otto.synapse.message.Header.responseHeader;
import static de.otto.synapse.message.Message.message;
import static java.lang.String.valueOf;
import static java.time.Duration.ZERO;
import static java.time.Instant.now;
import static java.util.Arrays.asList;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.fail;

/**
 * Tests that must be successful for all MessageStore implementations
 */
@RunWith(Parameterized.class)
public class MessageStoreTest {

    @Parameters
    public static Iterable<? extends Supplier<MessageStore>> messageStores() {
        return asList(
                InMemoryMessageStore::new,
                () -> new InMemoryRingBufferMessageStore(10000),
                CompactingInMemoryMessageStore::new,
                CompactingConcurrentMapMessageStore::new
        );
    }

    @Parameter
    public Supplier<MessageStore> messageStoreBuilder;

    @Test
    public void shouldAddMessagesWithoutHeaders() {
        final MessageStore messageStore = messageStoreBuilder.get();
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
    public void shouldKeepInsertionOrderOfMessages() {
        final MessageStore messageStore = messageStoreBuilder.get();
        final ExecutorService executorService = newFixedThreadPool(10);
        final CompletableFuture[] completion = new CompletableFuture[5];
        for (int shard=0; shard<5; ++shard) {
            final String shardId = valueOf(shard);
            completion[shard] = CompletableFuture.runAsync(() -> {
                for (int pos = 0; pos < 10000; ++pos) {
                    messageStore.add(message(valueOf(pos), responseHeader(fromPosition("shard-" + shardId, ZERO, valueOf(pos)), now()), "some payload"));
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
        assertThat(messageStore.size(), isOneOf(10000, 50000));
    }

    // TODO: @Test
    public void shouldCalculateDurationBehindProperly() {
        fail("not yet implemented");
    }
}