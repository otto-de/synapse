package de.otto.synapse.endpoint.receiver.aws;


import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.channel.ChannelDurationBehind;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.consumer.MessageDispatcher;
import de.otto.synapse.message.Message;
import de.otto.synapse.translator.MessageTranslator;
import org.junit.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.Record;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.regex.Pattern;

import static com.google.common.collect.ImmutableList.of;
import static de.otto.synapse.channel.ChannelDurationBehind.channelDurationBehind;
import static de.otto.synapse.channel.ShardPosition.fromHorizon;
import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static de.otto.synapse.message.Header.responseHeader;
import static de.otto.synapse.message.Message.message;
import static java.nio.charset.Charset.forName;
import static java.time.Instant.now;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.*;

public class KinesisMessageLogResponseTest {

    private Instant now;

    @Test
    public void shouldCalculateChannelPosition() {
        final GetRecordsResponse recordsResponse = mock(GetRecordsResponse.class);
        when(recordsResponse.records()).thenReturn(emptyList());
        final KinesisMessageLogResponse response = new KinesisMessageLogResponse(
                "foo",
                of(
                        new KinesisShardResponse("foo", fromHorizon("foo"), recordsResponse, 1000),
                        new KinesisShardResponse("foo", fromPosition("bar", "42"), recordsResponse, 1000)
                )
        );
        assertThat(response.getChannelPosition(), is(ChannelPosition.channelPosition(fromHorizon("foo"), fromPosition("bar", "42"))));
    }

    @Test
    public void shouldReturnMessages() {
        final GetRecordsResponse recordsResponse = mock(GetRecordsResponse.class);
        now = now();
        when(recordsResponse.records()).thenReturn(asList(
                Record.builder().partitionKey("a").sequenceNumber("1").approximateArrivalTimestamp(now).build(),
                Record.builder().partitionKey("b").sequenceNumber("2").approximateArrivalTimestamp(now).build()
        ));
        final KinesisMessageLogResponse response = new KinesisMessageLogResponse(
                "foo",
                of(
                        new KinesisShardResponse("foo", fromHorizon("foo"), recordsResponse, 1000)
                )
        );
        assertThat(response.getMessages(), contains(
                message("a", responseHeader(fromPosition("foo", "1"), now), null),
                message("b", responseHeader(fromPosition("foo", "2"), now), null)));
    }

    @Test
    public void shouldGetTranslatedMessages() {
        final GetRecordsResponse recordsResponse = mock(GetRecordsResponse.class);
        now = now();
        when(recordsResponse.records()).thenReturn(asList(
                Record.builder()
                        .partitionKey("a")
                        .sequenceNumber("1")
                        .approximateArrivalTimestamp(now)
                        .data(SdkBytes.fromByteArray("{\"foo\":\"first\"}".getBytes(forName("UTF8"))))
                        .build(),
                Record.builder()
                        .partitionKey("b")
                        .sequenceNumber("2")
                        .approximateArrivalTimestamp(now)
                        .data(SdkBytes.fromByteArray("{\"foo\":\"second\"}".getBytes(forName("UTF8"))))
                        .build()
        ));
        final KinesisMessageLogResponse response = new KinesisMessageLogResponse(
                "foo",
                of(
                        new KinesisShardResponse("foo", fromHorizon("foo"), recordsResponse, 1000)
                )
        );
        final MessageTranslator<TestPayload> messageTranslator = MessageTranslator.of((payload -> {
            try {
                final String json = Objects.toString(payload, "{}");
                return new ObjectMapper().readValue(json, TestPayload.class);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }));
        assertThat(response.getMessages(messageTranslator), contains(
                message("a", responseHeader(fromPosition("foo", "1"), now), new TestPayload("first")),
                message("b", responseHeader(fromPosition("foo", "2"), now), new TestPayload("second"))
        ));
    }

    @Test
    public void shouldDispatchMessages() {
        final GetRecordsResponse recordsResponse = mock(GetRecordsResponse.class);
        now = now();
        when(recordsResponse.records()).thenReturn(asList(
                Record.builder().partitionKey("a").sequenceNumber("1").approximateArrivalTimestamp(now).data(SdkBytes.fromByteArray("{\"foo\":\"first\"}".getBytes(forName("UTF8")))).build(),
                Record.builder().partitionKey("b").sequenceNumber("2").approximateArrivalTimestamp(now).data(SdkBytes.fromByteArray("{\"foo\":\"second\"}".getBytes(forName("UTF8")))).build()
        ));
        final KinesisMessageLogResponse response = new KinesisMessageLogResponse(
                "foo",
                of(
                        new KinesisShardResponse("foo", fromHorizon("foo"), recordsResponse, 1000)
                )
        );
        final MessageConsumer<TestPayload> consumer = spy(testMessageConsumer());
        response.dispatchMessages(new MessageDispatcher(new ObjectMapper(), singletonList(consumer)));
        verify(consumer).accept(message("a", responseHeader(fromPosition("foo", "1"), now), new TestPayload("first")));
        verify(consumer).accept(message("b", responseHeader(fromPosition("foo", "2"), now), new TestPayload("second")));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailToCreateResponseWithoutAnyShardResponses() {
        final GetRecordsResponse recordsResponse = mock(GetRecordsResponse.class);
        when(recordsResponse.records()).thenReturn(emptyList());
        new KinesisMessageLogResponse("foo", of());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailToCreateResponseFromDifferentChannels() {
        final GetRecordsResponse recordsResponse = mock(GetRecordsResponse.class);
        when(recordsResponse.records()).thenReturn(emptyList());
        new KinesisMessageLogResponse(
                "foo",
                of(
                        new KinesisShardResponse("foo", fromHorizon("foo"), recordsResponse, 1000),
                        new KinesisShardResponse("bar", fromHorizon("bar"), recordsResponse, 1000)
                )
        );
    }

    @Test
    public void shouldCalculateDurationBehind() {
        final KinesisMessageLogResponse response = new KinesisMessageLogResponse("foo", of(
                someShardResponse("first", 42000L),
                someShardResponse("second", 0L))
        );

        final ChannelDurationBehind expectedDurationBehind = channelDurationBehind()
                .with("first", Duration.ofSeconds(42))
                .with("second", Duration.ofSeconds(0))
                .build();
        assertThat(response.getChannelDurationBehind(), is(expectedDurationBehind));
    }

    @Test
    public void shouldReturnShardNames() {
        final KinesisMessageLogResponse response = new KinesisMessageLogResponse("foo", of(
                someShardResponse("first", 0L),
                someShardResponse("second", 0L))
        );

        assertThat(response.getShardNames(), containsInAnyOrder("first", "second"));
    }

    private KinesisShardResponse someShardResponse(final String shardName,
                                                   final long millisBehind) {
        final GetRecordsResponse recordsResponse = mock(GetRecordsResponse.class);
        when(recordsResponse.millisBehindLatest()).thenReturn(millisBehind);
        return new KinesisShardResponse(
                "foo",
                fromPosition(shardName, "5"),

                recordsResponse, 0L);
    }

    private MessageConsumer<TestPayload> testMessageConsumer() {
        return new MessageConsumer<TestPayload>() {
            @Nonnull
            @Override
            public Class<TestPayload> payloadType() {
                return TestPayload.class;
            }

            @Nonnull
            @Override
            public Pattern keyPattern() {
                return Pattern.compile(".*");
            }

            @Override
            public void accept(Message<TestPayload> message) {
            }
        };
    }

    private static class TestPayload {
        @JsonProperty
        public String foo;

        public TestPayload() {
        }

        public TestPayload(final String foo) {
            this.foo = foo;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TestPayload that = (TestPayload) o;
            return Objects.equals(foo, that.foo);
        }

        @Override
        public int hashCode() {

            return Objects.hash(foo);
        }

        @Override
        public String toString() {
            return "TestPayload{" +
                    "foo='" + foo + '\'' +
                    '}';
        }
    }

}

