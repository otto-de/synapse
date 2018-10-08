package de.otto.synapse.endpoint.receiver.aws;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static com.google.common.collect.ImmutableList.of;
import static de.otto.synapse.endpoint.receiver.aws.KinesisStreamInfo.copyOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class KinesisStreamInfoTest {

    @Test
    public void shouldCreateStreamInfo() {
        KinesisStreamInfo streamInfo = new KinesisStreamInfo("foo", "arn:foo", of(new KinesisShardInfo("bar", true)));
        assertThat(streamInfo.getChannelName(), is("foo"));
        assertThat(streamInfo.getArn(), is("arn:foo"));
        assertThat(streamInfo.getShardInfo(), contains(new KinesisShardInfo("bar", true)));
    }

    @Test
    public void shouldCreateCopyOfBuilder() {
        KinesisStreamInfo streamInfo = KinesisStreamInfo.builder().withChannelName("foo").withArn("arn:foo").withShardInfo(of(
                new KinesisShardInfo("first", true),
                new KinesisShardInfo("second", false)
        )).build();

        assertThat(copyOf(streamInfo).build(), is(streamInfo));
    }

    @Test
    public void shouldModifyFieldsInCopiedInfo() {
        KinesisStreamInfo streamInfo = KinesisStreamInfo.builder().withChannelName("foo").withArn("arn:foo").withShardInfo(of(
                new KinesisShardInfo("first", true),
                new KinesisShardInfo("second", false)
        )).build();

        KinesisStreamInfo modified = copyOf(streamInfo)
                .withChannelName("bar")
                .withArn("arn:bar")
                .withShard("third", true)
                .build();

        assertThat(modified.getChannelName(), is("bar"));
        assertThat(modified.getArn(), is("arn:bar"));
        assertThat(modified.getShardInfo(), contains(
                new KinesisShardInfo("first", true),
                new KinesisShardInfo("second", false),
                new KinesisShardInfo("third", true)
        ));
    }
}