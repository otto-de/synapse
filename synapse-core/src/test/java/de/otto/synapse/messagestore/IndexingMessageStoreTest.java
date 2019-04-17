package de.otto.synapse.messagestore;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import de.otto.synapse.channel.ShardPosition;
import de.otto.synapse.message.Header;
import de.otto.synapse.message.Key;
import de.otto.synapse.message.TextMessage;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.google.common.collect.Sets.newHashSet;
import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static de.otto.synapse.channel.StartFrom.POSITION;
import static de.otto.synapse.messagestore.Index.CHANNEL_NAME;
import static de.otto.synapse.messagestore.Indexers.channelNameIndexer;
import static java.lang.String.valueOf;
import static java.util.Arrays.asList;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Tests that must be successful for all MessageStore implementations
 */
@RunWith(Parameterized.class)
public class IndexingMessageStoreTest {

    @Parameters
    public static Iterable<? extends Supplier<MessageStore>> channelIndexedMessageStore() {
        return asList(
                () -> new OnHeapIndexingMessageStore(channelNameIndexer()),
                () -> new OffHeapIndexingMessageStore("test", channelNameIndexer())
        );
    }

    @Parameter
    public Supplier<MessageStore> channelIndexedMessageStore;

    @Test
    public void shouldReturnIndexes() {
        ImmutableSet<Index> expectedIndexes = ImmutableSet.of(CHANNEL_NAME);
        ImmutableSet<Index> indexes = channelIndexedMessageStore.get().getIndexes();
        assertThat(indexes, is(expectedIndexes));
    }

    @Test
    public void shouldReturnEmptyStreamForNonExistingIndex() {
        Stream<MessageStoreEntry> stream = channelIndexedMessageStore.get().stream(Index.valueOf("unknown"), "42");
        assertThat(stream.count(), is(0L));
    }

    @SuppressWarnings("Duplicates")
    @Test
    public void shouldReturnFilterValuesFromStream() {
        final MessageStore messageStore = channelIndexedMessageStore.get();

        final String partitionKey = UUID.randomUUID().toString();

        messageStore.add(MessageStoreEntry.of(
                "one",
                TextMessage.of(partitionKey, "1"))
        );

        final MessageStoreEntry entry = messageStore
                .stream()
                .findFirst()
                .get();

        assertThat(entry.getFilterValues(), is(ImmutableMap.of(
                CHANNEL_NAME, "one"
        )));
    }

    @SuppressWarnings("Duplicates")
    @Test
    public void shouldStreamMessagesMatchingSingleIndex() {
        final MessageStore messageStore = channelIndexedMessageStore.get();
        for (int i=0; i<10; ++i) {
            messageStore.add(MessageStoreEntry.of(i%2==0 ? "even-channel" : "odd-channel", TextMessage.of(Key.of(valueOf(i)), "some payload")));
        }

        final Set<String> expectedKeys = newHashSet("0", "2", "4", "6", "8");
        messageStore.stream(Index.CHANNEL_NAME, "even-channel").forEach(entry -> {
            assertThat(expectedKeys.contains(entry.getTextMessage().getKey().partitionKey()), is(true));
        });
        assertThat(messageStore.size(), is(10));
    }

    @SuppressWarnings("Duplicates")
    @Test
    public void shouldKeepInsertionOrderOfIndexedMessages() {
        final MessageStore messageStore = channelIndexedMessageStore.get();
        final ExecutorService executorService = newFixedThreadPool(10);
        final CompletableFuture[] completion = new CompletableFuture[5];
        for (int shard=0; shard<5; ++shard) {
            final String shardId = valueOf(shard);
            completion[shard] = CompletableFuture.runAsync(() -> {
                for (int pos = 0; pos < 100; ++pos) {
                    messageStore.add(MessageStoreEntry.of(
                            "first",
                            TextMessage.of(
                                    Key.of(shardId + "#" + pos),
                                    Header.of(fromPosition("shard-" + shardId, valueOf(pos))),
                                    "some payload")));
                    messageStore.add(MessageStoreEntry.of(
                            "second",
                            TextMessage.of(
                                    Key.of(shardId + "#" + pos),
                                    Header.of(fromPosition("shard-" + shardId, valueOf(pos))),
                                    "some payload")));
                    assertThat(messageStore.getLatestChannelPosition("first").shard("shard-" + shardId).startFrom(), is(POSITION));
                    assertThat(messageStore.getLatestChannelPosition("second").shard("shard-" + shardId).position(), is(valueOf(pos)));
                }

            }, executorService);
        }
        allOf(completion).join();
        final Map<String, Integer> lastPositions = new HashMap<>();
        messageStore.stream(Index.CHANNEL_NAME, "first").forEach(entry -> {
            final Header header = entry.getTextMessage().getHeader();
            if (header.getShardPosition().isPresent()) {
                final ShardPosition shard = header.getShardPosition().get();
                final Integer pos = Integer.valueOf(shard.position());
                lastPositions.putIfAbsent(shard.shardName(), pos);
                assertThat(lastPositions.get(shard.shardName()), is(lessThanOrEqualTo(pos)));
            }
        });
        assertThat(messageStore.size(), is(1000));
    }

}