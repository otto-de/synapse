package de.otto.synapse.message;

import com.google.common.collect.ImmutableMap;
import de.otto.synapse.channel.ShardPosition;
import org.junit.Test;

import java.time.Instant;

import static de.otto.synapse.message.Message.message;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class TextMessageTest {

    @Test
    public void shouldHaveEqualsAndHashCode() {
        Instant now = Instant.now();
        TextMessage first = TextMessage.of(
                Key.of("foo", "bar"),
                Header.of(ShardPosition.fromTimestamp("shard", now), ImmutableMap.of("attr", "value")),
                "some payload");
        TextMessage second = TextMessage.of(
                Key.of("foo", "bar"),
                Header.of(ShardPosition.fromTimestamp("shard", now), ImmutableMap.of("attr", "value")),
                "some payload");
        assertThat(first, is(second));
        assertThat(second, is(first));
        assertThat(first.hashCode(), is(second.hashCode()));
    }

    @Test
    public void shouldBeEqualToMessage() {
        Instant now = Instant.now();
        TextMessage first = TextMessage.of(
                Key.of("foo", "bar"),
                Header.of(ShardPosition.fromTimestamp("shard", now), ImmutableMap.of("attr", "value")),
                "some payload");
        Message<String> second = message(
                Key.of("foo", "bar"),
                Header.of(ShardPosition.fromTimestamp("shard", now), ImmutableMap.of("attr", "value")),
                "some payload");
        assertThat(first, is(second));
        assertThat(second, is(first));
        assertThat(first.hashCode(), is(second.hashCode()));
    }

}