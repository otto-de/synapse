package de.otto.synapse.message.aws;

import de.otto.synapse.message.Message;
import org.junit.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.model.Record;

import java.time.Instant;
import java.util.Optional;

import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static de.otto.synapse.message.aws.KinesisMessage.kinesisMessage;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

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

}
