package de.otto.synapse.message;

import org.junit.Test;

import java.time.Instant;

import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static de.otto.synapse.message.Header.of;
import static de.otto.synapse.message.Message.message;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class MessageTest {

    @Test
    public void shouldBuildMessageWithHeader() {
        final Message<String> message = message(
                "42",
                of(fromPosition("some-channel", "00001")),
                "ßome dätä"
        );
        assertThat(message.getKey(), is(Key.of("42")));
        assertThat(message.getPayload(), is("ßome dätä"));
        assertThat(message.getHeader().getShardPosition().get().shardName(), is("some-channel"));
        assertThat(message.getHeader().getShardPosition().get().position(), is("00001"));
    }

    @Test
    public void shouldBuildMessageWithoutHeader() {
        final Instant now = Instant.now();
        final Message<String> message = message(
                "42",
                "ßome dätä"
        );
        assertThat(message.getKey(), is(Key.of("42")));
        assertThat(message.getPayload(), is("ßome dätä"));
        assertThat(message.getHeader().getShardPosition().isPresent(), is(false));
    }
}
