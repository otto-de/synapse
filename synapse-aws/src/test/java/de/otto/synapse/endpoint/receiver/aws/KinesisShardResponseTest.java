package de.otto.synapse.endpoint.receiver.aws;

import org.junit.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.Record;

import java.time.Duration;
import java.time.Instant;

import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static de.otto.synapse.message.Header.responseHeader;
import static de.otto.synapse.message.Message.message;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.Instant.now;
import static java.time.temporal.ChronoUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

public class KinesisShardResponseTest {

    @Test
    public void shouldImplementEqualsAndHashCode() {
        final GetRecordsResponse response = GetRecordsResponse
                .builder()
                .records(Record.builder()
                        .sequenceNumber("1")
                        .partitionKey("first")
                        .approximateArrivalTimestamp(now())
                        .build())
                .nextShardIterator("nextIter")
                .millisBehindLatest(0L)
                .build();
        final KinesisShardResponse first = new KinesisShardResponse("channel", fromPosition("shard", "42"), response, 1234);
        final KinesisShardResponse second = new KinesisShardResponse("channel", fromPosition("shard", "42"), response, 1234);

        assertThat(first.equals(second), is(true));
        assertThat(first.hashCode(), is(second.hashCode()));
    }

    @Test
    public void shouldConvertRecordsToMessages() {
        final Instant firstArrival = now().minus(1, SECONDS);
        final Instant secondArrival = now();
        final GetRecordsResponse recordsResponse = GetRecordsResponse
                .builder()
                .records(
                        Record.builder()
                                .sequenceNumber("1")
                                .approximateArrivalTimestamp(firstArrival)
                                .partitionKey("first")
                                .data(SdkBytes.fromByteArray("content".getBytes(UTF_8)))
                                .build(),
                        Record.builder()
                                .sequenceNumber("2")
                                .approximateArrivalTimestamp(secondArrival)
                                .partitionKey("second")
                                .data(SdkBytes.fromByteArray("content".getBytes(UTF_8)))
                                .build()
                        )
                .nextShardIterator("nextIter")
                .millisBehindLatest(1L)
                .build();
        final KinesisShardResponse response = new KinesisShardResponse("channel", fromPosition("shard", "42"), recordsResponse, 1234);

        assertThat(response.getChannelName(), is("channel"));
        assertThat(response.getShardName(), is("shard"));
        assertThat(response.getDurationBehind(), is(Duration.ofMillis(1L)));
        assertThat(response.getRuntime(), is(1234L));
        assertThat(response.getShardPosition(), is(fromPosition("shard", "42")));
        assertThat(response.getMessages(), contains(
                message("first", responseHeader(fromPosition("shard", "1"), firstArrival), "content"),
                message("second", responseHeader(fromPosition("shard", "2"), secondArrival), "content")
        ));
    }
}