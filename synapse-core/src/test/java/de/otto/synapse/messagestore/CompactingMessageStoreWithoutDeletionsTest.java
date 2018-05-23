package de.otto.synapse.messagestore;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static de.otto.synapse.channel.ChannelPosition.channelPosition;
import static de.otto.synapse.channel.ChannelPosition.fromHorizon;
import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static de.otto.synapse.message.Header.responseHeader;
import static de.otto.synapse.message.Message.message;
import static java.lang.String.valueOf;
import static java.time.Duration.ZERO;
import static java.time.Duration.ofMillis;
import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * Tests specific for all compacting MessageStore implementations
 */
@RunWith(Parameterized.class)
public class CompactingMessageStoreWithoutDeletionsTest {

    @Parameters
    public static Iterable<? extends Supplier<MessageStore>> messageStores() {
        return asList(
                () -> new CompactingInMemoryMessageStore(false),
                () -> new CompactingConcurrentMapMessageStore(false),
                () -> new CompactingConcurrentMapMessageStore(false, new ConcurrentHashMap<>())
        );
    }

    @Parameter
    public Supplier<MessageStore> messageStoreBuilder;

    @Test
    public void shouldNotRemoveMessagesWithoutChannelPositionWithNullPayload() {
        final MessageStore messageStore = messageStoreBuilder.get();
        for (int i=0; i<10; ++i) {
            messageStore.add(message(valueOf(i), "some payload"));
        }
        for (int i=0; i<10; ++i) {
            messageStore.add(message(valueOf(i), null));
        }
        assertThat(messageStore.size(), is(10));
        assertThat(messageStore.getLatestChannelPosition(), is(fromHorizon()));
    }

    @Test
    public void shouldNotRemoveMessagesWithNullPayload() {
        final MessageStore messageStore = messageStoreBuilder.get();
        final Instant yesterday = Instant.now().minus(1, ChronoUnit.DAYS);
        final Instant now = Instant.now().minus(1, ChronoUnit.DAYS);
        for (int i=0; i<10; ++i) {
            messageStore.add(message(
                    valueOf(i),
                    responseHeader(fromPosition("foo", ofMillis(42), valueOf(i)), yesterday),
                    "some foo payload"));
            messageStore.add(message(
                    valueOf(i),
                    responseHeader(fromPosition("bar", ofMillis(44), valueOf(i)), yesterday),
                    "some bar payload"));
        }
        for (int i=0; i<10; ++i) {
            messageStore.add(message(valueOf(i), responseHeader(fromPosition("foo", ZERO, valueOf(20 + i)), now), null));
            messageStore.add(message(valueOf(i), responseHeader(fromPosition("bar", ZERO, valueOf(42 + i)), now), null));
        }
        assertThat(messageStore.getLatestChannelPosition(), is(channelPosition(fromPosition("foo", ZERO, "29"), fromPosition("bar", ZERO, "51"))));
        assertThat(messageStore.size(), is(20));
        assertThat(messageStore.stream().count(), is(20L));
    }

}