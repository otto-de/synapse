package de.otto.synapse.messagestore;

import de.otto.synapse.message.Key;
import de.otto.synapse.message.TextMessage;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static de.otto.synapse.channel.ChannelPosition.channelPosition;
import static de.otto.synapse.channel.ChannelPosition.fromHorizon;
import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static de.otto.synapse.message.Header.of;
import static java.lang.String.valueOf;
import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * Tests specific for all compacting MessageStore implementations
 */
@RunWith(Parameterized.class)
public class CompactingMessageStoreWithoutDeletionsTest {

    @Parameters
    public static Iterable<? extends Supplier<WritableMessageStore>> messageStores() {
        return asList(
                () -> new CompactingInMemoryMessageStore(false),
                () -> new CompactingConcurrentMapMessageStore(false),
                () -> new CompactingConcurrentMapMessageStore(false, new ConcurrentHashMap<>())
        );
    }

    @Parameter
    public Supplier<WritableMessageStore> messageStoreBuilder;

    @Test
    public void shouldNotRemoveMessagesWithoutChannelPositionWithNullPayload() {
        final WritableMessageStore messageStore = messageStoreBuilder.get();
        for (int i=0; i<10; ++i) {
            messageStore.add(TextMessage.of(Key.of(valueOf(i)), "some payload"));
        }
        for (int i=0; i<10; ++i) {
            messageStore.add(TextMessage.of(Key.of(valueOf(i)), null));
        }
        assertThat(messageStore.size(), is(10));
        assertThat(messageStore.getLatestChannelPosition(), is(fromHorizon()));
    }

    @Test
    public void shouldNotRemoveMessagesWithNullPayload() {
        final WritableMessageStore messageStore = messageStoreBuilder.get();
        for (int i=0; i<10; ++i) {
            messageStore.add(TextMessage.of(
                    Key.of(valueOf(i), "foo:" + i),
                    of(fromPosition("foo", valueOf(i))),
                    "some foo payload"));
            messageStore.add(TextMessage.of(
                    Key.of(valueOf(i), "bar:" + i),
                    of(fromPosition("bar", valueOf(i))),
                    "some bar payload"));
        }
        for (int i=0; i<10; ++i) {
            messageStore.add(TextMessage.of(Key.of(valueOf(i), "foo:" + i), of(fromPosition("foo", valueOf(20 + i))), null));
            messageStore.add(TextMessage.of(Key.of(valueOf(i), "bar:" + i), of(fromPosition("bar", valueOf(42 + i))), null));
        }
        assertThat(messageStore.getLatestChannelPosition(), is(channelPosition(fromPosition("foo", "29"), fromPosition("bar", "51"))));
        assertThat(messageStore.size(), is(20));
        assertThat(messageStore.stream().count(), is(20L));
    }

}