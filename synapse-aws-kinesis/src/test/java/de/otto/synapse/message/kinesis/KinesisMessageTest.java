package de.otto.synapse.message.kinesis;

import com.google.common.collect.ImmutableMap;
import de.otto.synapse.message.Message;
import org.junit.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.model.Record;

import java.time.Instant;
import java.util.Optional;

import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static de.otto.synapse.message.kinesis.KinesisMessage.kinesisMessage;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class KinesisMessageTest {

    @Test
    public void shouldBuildKinesisMessage() {
        final Instant now = Instant.now();
        final Record record = Record.builder()
                .partitionKey("42")
                .data(SdkBytes.fromString("ßome dätä",UTF_8))
                .approximateArrivalTimestamp(now)
                .sequenceNumber("00001")
                .build();
        final Message<String> message = kinesisMessage(
                "some-shard",
                record);
        assertThat(message.getKey(), is("42"));
        assertThat(message.getPayload(), is("ßome dätä"));
        assertThat(message.getHeader().getArrivalTimestamp(), is(now));
        assertThat(message.getHeader().getShardPosition(), is(Optional.of(fromPosition("some-shard", "00001"))));
    }

    @Test
    public void shouldBuildKinesisMessageV2() {
        final String json = "{\"_synapse_msg_format\":\"v2\","
                + "\"_synapse_msg_headers\":{\"attr\":\"value\"},"
                + "\"_synapse_msg_payload\":{\"some\":\"payload\"}}";

        final Instant now = Instant.now();
        final Record record = Record.builder()
                .partitionKey("42")
                .data(SdkBytes.fromString(json,UTF_8))
                .approximateArrivalTimestamp(now)
                .sequenceNumber("00001")
                .build();
        final Message<String> message = kinesisMessage(
                "some-shard",
                record);
        assertThat(message.getKey(), is("42"));
        assertThat(message.getPayload(), is("{\"some\":\"payload\"}"));
        assertThat(message.getHeader().getArrivalTimestamp(), is(now));
        assertThat(message.getHeader().getShardPosition(), is(Optional.of(fromPosition("some-shard", "00001"))));
        assertThat(message.getHeader().getAttributes(), is(ImmutableMap.of("attr", "value")));
    }

    @Test
    public void shouldBuildKinesisDeletionMessageV2() {
        final String json = "{\n   \"_synapse_msg_format\"  :  \"v2\",     "
                + "\"_synapse_msg_headers\":{\"attr\":\"value\"},"
                + "\"_synapse_msg_payload\":null}";

        final Instant now = Instant.now();
        final Record record = Record.builder()
                .partitionKey("42")
                .data(SdkBytes.fromString(json,UTF_8))
                .approximateArrivalTimestamp(now)
                .sequenceNumber("00001")
                .build();
        final Message<String> message = kinesisMessage(
                "some-shard",
                record);
        assertThat(message.getKey(), is("42"));
        assertThat(message.getPayload(), is(nullValue()));
        assertThat(message.getHeader().getArrivalTimestamp(), is(now));
        assertThat(message.getHeader().getShardPosition(), is(Optional.of(fromPosition("some-shard", "00001"))));
        assertThat(message.getHeader().getAttributes(), is(ImmutableMap.of("attr", "value")));
    }

    @Test
    public void shouldBuildKinesisDeletionMessageWithoutHeadersV2() {
        final String json = "{\"_synapse_msg_format\":\"v2\","
                + "\"_synapse_msg_headers\":{},"
                + "\"_synapse_msg_payload\":null}";

        final Instant now = Instant.now();
        final Record record = Record.builder()
                .partitionKey("42")
                .data(SdkBytes.fromString(json,UTF_8))
                .approximateArrivalTimestamp(now)
                .sequenceNumber("00001")
                .build();
        final Message<String> message = kinesisMessage(
                "some-shard",
                record);
        assertThat(message.getKey(), is("42"));
        assertThat(message.getPayload(), is(nullValue()));
        assertThat(message.getHeader().getArrivalTimestamp(), is(now));
        assertThat(message.getHeader().getShardPosition(), is(Optional.of(fromPosition("some-shard", "00001"))));
        assertThat(message.getHeader().getAttributes(), is(ImmutableMap.of()));
    }

    @Test
    public void shouldBuildMinimalKinesisDeletionMessageV2() {
        final String json = "{\"_synapse_msg_format\":\"v2\"}";

        final Instant now = Instant.now();
        final Record record = Record.builder()
                .partitionKey("42")
                .data(SdkBytes.fromString(json,UTF_8))
                .approximateArrivalTimestamp(now)
                .sequenceNumber("00001")
                .build();
        final Message<String> message = kinesisMessage(
                "some-shard",
                record);
        assertThat(message.getKey(), is("42"));
        assertThat(message.getPayload(), is(nullValue()));
        assertThat(message.getHeader().getArrivalTimestamp(), is(now));
        assertThat(message.getHeader().getShardPosition(), is(Optional.of(fromPosition("some-shard", "00001"))));
        assertThat(message.getHeader().getAttributes(), is(ImmutableMap.of()));
    }

}
