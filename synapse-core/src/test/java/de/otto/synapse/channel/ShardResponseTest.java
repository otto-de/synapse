package de.otto.synapse.channel;

import de.otto.synapse.message.Message;
import org.junit.Test;

import java.time.Duration;

import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static de.otto.synapse.message.Message.message;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class ShardResponseTest {

    @Test
    public void shouldImplementEqualsAndHashCode() {
        final Message<String> message = message("", null);
        final ShardResponse first = ShardResponse.shardResponse(fromPosition("shard", "42"), Duration.ofMillis(4711), message);
        final ShardResponse second = ShardResponse.shardResponse(fromPosition("shard", "42"), Duration.ofMillis(4711), message);

        assertThat(first.equals(second), is(true));
        assertThat(first.hashCode(), is(second.hashCode()));
    }

}