package de.otto.synapse.messagestore;

import com.google.common.collect.ImmutableList;
import de.otto.synapse.message.TextMessage;
import org.junit.Test;

import static de.otto.synapse.messagestore.Index.CHANNEL_NAME;
import static de.otto.synapse.messagestore.Index.PARTITION_KEY;
import static de.otto.synapse.messagestore.Indexers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class IndexersTest {

    @Test
    public void shouldCalculatePartitionKeyIndex() {
        final Indexer indexer = partitionKeyIndexer();
        assertThat(indexer.getIndexes(), contains(PARTITION_KEY));
        assertThat(indexer.supports(PARTITION_KEY), is(true));
        assertThat(indexer.supports(CHANNEL_NAME), is(false));
        assertThat(indexer.calc(PARTITION_KEY, MessageStoreEntry.of(
                "channel-name",
                TextMessage.of("foo", null))), is("foo"));
    }

    @Test
    public void shouldCalculateCompositeIndex() {
        final Indexer indexer = composite(ImmutableList.of(partitionKeyIndexer(), channelNameIndexer()));
        assertThat(indexer.getIndexes(), containsInAnyOrder(PARTITION_KEY, CHANNEL_NAME));
        assertThat(indexer.supports(PARTITION_KEY), is(true));
        assertThat(indexer.supports(CHANNEL_NAME), is(true));
        assertThat(indexer.calc(PARTITION_KEY, MessageStoreEntry.of(
                "channel-name",
                TextMessage.of("foo", null))), is("foo"));
        assertThat(indexer.calc(CHANNEL_NAME, MessageStoreEntry.of(
                "channel-name",
                TextMessage.of("foo", null))), is("channel-name"));
    }
}