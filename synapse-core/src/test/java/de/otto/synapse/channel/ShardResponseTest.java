package de.otto.synapse.channel;

import de.otto.synapse.message.TextMessage;
import org.junit.Test;

import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static de.otto.synapse.channel.ShardResponse.shardResponse;
import static de.otto.synapse.message.Key.NO_KEY;
import static java.time.Duration.ofMillis;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class ShardResponseTest {

    @Test
    public void shouldImplementEqualsAndHashCode() {
        final TextMessage message = TextMessage.of(NO_KEY, null);
        final ShardResponse first = shardResponse(fromPosition("shard", "42"), ofMillis(4711), message);
        final ShardResponse second = shardResponse(fromPosition("shard", "42"), ofMillis(4711), message);

        assertThat(first.equals(second), is(true));
        assertThat(first.hashCode(), is(second.hashCode()));
    }

}