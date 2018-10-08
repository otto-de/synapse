package de.otto.synapse.endpoint.receiver.aws;

import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class KinesisShardInfoTest {

    @Test
    public void shouldCreateShardInfo() {
        KinesisShardInfo shardInfo = new KinesisShardInfo("test", true);
        assertThat(shardInfo.getShardName(), is("test"));
        assertThat(shardInfo.isOpen(), is(true));
        assertThat(shardInfo, is(new KinesisShardInfo("test", true)));
        assertThat(shardInfo.hashCode(), is(new KinesisShardInfo("test", true).hashCode()));
        assertThat(shardInfo.toString(), is(new KinesisShardInfo("test", true).toString()));
    }

}