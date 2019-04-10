package de.otto.synapse.messagestore;

import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.StartFrom;
import de.otto.synapse.message.Key;
import de.otto.synapse.message.TextMessage;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static de.otto.synapse.message.Header.of;
import static java.lang.String.valueOf;
import static java.util.Arrays.asList;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@RunWith(Parameterized.class)

/**
 * Tests specific for all non-compacting MessageStore implementations
 */
public class NonCompactingMessageStoreTest {

    @Parameterized.Parameters
    public static Iterable<? extends Supplier<MessageStore>> messageStores() {
        return asList(
                () -> new InMemoryMessageStore(),
                () -> new InMemoryRingBufferMessageStore(50000)
        );
    }

    @Parameter
    public Supplier<MessageStore> messageStoreBuilder;

    @Test
    public void shouldStreamAllMessages() {
        final MessageStore messageStore = messageStoreBuilder.get();
        messageStore.add(MessageStoreEntry.of("one", TextMessage.of("1", "1")));
        messageStore.add(MessageStoreEntry.of("two", TextMessage.of("2", "2")));
        messageStore.add(MessageStoreEntry.of("one", TextMessage.of("3", "3")));

        final List<String> channelNames = messageStore
                .stream()
                .map(MessageStoreEntry::getChannelName)
                .collect(Collectors.toList());
        final List<String> messageKeys = messageStore
                .stream()
                .map(MessageStoreEntry::getTextMessage)
                .map(TextMessage::getKey)
                .map(Key::partitionKey)
                .collect(Collectors.toList());
        assertThat(channelNames, contains("one", "two", "one"));
        assertThat(messageKeys, contains("1", "2", "3"));
    }

    @SuppressWarnings("Duplicates")
    @Test
    public void shouldCalculateUncompactedChannelPositionsForSingleChannel() {
        final MessageStore messageStore = messageStoreBuilder.get();
        final ExecutorService executorService = newFixedThreadPool(10);
        final CompletableFuture[] completion = new CompletableFuture[5];
        for (int shard=0; shard<5; ++shard) {
            final String shardId = valueOf(shard);
            completion[shard] = CompletableFuture.runAsync(() -> {
                for (int pos = 0; pos < 10000; ++pos) {
                    messageStore.add(MessageStoreEntry.of("", TextMessage.of(Key.of(valueOf(pos)), of(fromPosition("shard-" + shardId, valueOf(pos))), "some payload")));
                    assertThat(messageStore.getLatestChannelPosition("").shard("shard-" + shardId).startFrom(), is(StartFrom.POSITION));
                    assertThat(messageStore.getLatestChannelPosition("").shard("shard-" + shardId).position(), is(valueOf(pos)));
                }

            }, executorService);
        }
        allOf(completion).join();
        ChannelPosition position = messageStore.getLatestChannelPosition("");
        assertThat(position.shards(), containsInAnyOrder("shard-0", "shard-1", "shard-2", "shard-3", "shard-4"));
        assertThat(position.shard("shard-0").position(), is("9999"));
        assertThat(position.shard("shard-1").position(), is("9999"));
        assertThat(position.shard("shard-2").position(), is("9999"));
        assertThat(position.shard("shard-3").position(), is("9999"));
        assertThat(position.shard("shard-4").position(), is("9999"));
        assertThat(messageStore.size(), is(50000));
    }

    @SuppressWarnings("Duplicates")
    @Test
    public void shouldCalculateUncompactedChannelPositionsForMultipleChannels() {
        final MessageStore messageStore = messageStoreBuilder.get();
        final ExecutorService executorService = newFixedThreadPool(10);
        final CompletableFuture[] completion = new CompletableFuture[10];
        for (int shard = 0; shard<5; ++shard) {
            final String shardId = valueOf(shard);
            completion[shard] = CompletableFuture.runAsync(() -> {
                for (int pos = 0; pos < 1000; ++pos) {
                    messageStore.add(MessageStoreEntry.of("first", TextMessage.of(Key.of(valueOf(pos)), of(fromPosition("shard-" + shardId, valueOf(pos))), "some payload")));
                    assertThat(messageStore.getLatestChannelPosition("first").shard("shard-" + shardId).startFrom(), is(StartFrom.POSITION));
                    assertThat(messageStore.getLatestChannelPosition("first").shard("shard-" + shardId).position(), is(valueOf(pos)));
                }

            }, executorService);
            completion[5+shard] = CompletableFuture.runAsync(() -> {
                for (int pos = 0; pos < 1000; ++pos) {
                    messageStore.add(MessageStoreEntry.of("second", TextMessage.of(Key.of(valueOf(pos)), of(fromPosition("shard-" + shardId, valueOf(pos))), "some payload")));
                    assertThat(messageStore.getLatestChannelPosition("second").shard("shard-" + shardId).startFrom(), is(StartFrom.POSITION));
                    assertThat(messageStore.getLatestChannelPosition("second").shard("shard-" + shardId).position(), is(valueOf(pos)));
                }

            }, executorService);
        }
        allOf(completion).join();
        ChannelPosition firstPos = messageStore.getLatestChannelPosition("first");
        assertThat(firstPos.shards(), containsInAnyOrder("shard-0", "shard-1", "shard-2", "shard-3", "shard-4"));
        assertThat(firstPos.shard("shard-0").position(), is("999"));
        assertThat(firstPos.shard("shard-1").position(), is("999"));
        assertThat(firstPos.shard("shard-2").position(), is("999"));
        assertThat(firstPos.shard("shard-3").position(), is("999"));
        assertThat(firstPos.shard("shard-4").position(), is("999"));
        ChannelPosition secondPos = messageStore.getLatestChannelPosition("second");
        assertThat(secondPos.shards(), containsInAnyOrder("shard-0", "shard-1", "shard-2", "shard-3", "shard-4"));
        assertThat(secondPos.shard("shard-0").position(), is("999"));
        assertThat(secondPos.shard("shard-1").position(), is("999"));
        assertThat(secondPos.shard("shard-2").position(), is("999"));
        assertThat(secondPos.shard("shard-3").position(), is("999"));
        assertThat(secondPos.shard("shard-4").position(), is("999"));
        assertThat(messageStore.size(), is(10000));
    }

}