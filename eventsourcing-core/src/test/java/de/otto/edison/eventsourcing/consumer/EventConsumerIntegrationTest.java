package de.otto.edison.eventsourcing.consumer;

import de.otto.edison.eventsourcing.annotation.EventSourceConsumer;
import de.otto.edison.eventsourcing.configuration.EventSourcingBootstrapConfiguration;
import de.otto.edison.eventsourcing.configuration.EventSourcingConfiguration;
import de.otto.edison.eventsourcing.configuration.SnapshotConfiguration;
import de.otto.edison.eventsourcing.s3.SnapshotReadService;
import org.awaitility.Awaitility;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

@RunWith(SpringRunner.class)
@ActiveProfiles("test")
@EnableAutoConfiguration
@ComponentScan(basePackages = {
        "de.otto.edison.eventsourcing.encryption",
})
@SpringBootTest(classes = {
        EventConsumerIntegrationTest.TestConfiguration.class,
        EventSourcingBootstrapConfiguration.class,
        EventSourcingConfiguration.class,
        SnapshotConfiguration.class
})
public class EventConsumerIntegrationTest {

    static List<String> allReceivedEventKeys = new ArrayList<>();
    static List<String> receivedAppleEventKeys = new ArrayList<>();
    static List<String> receivedBananaEventKeys = new ArrayList<>();

    @Test
    public void shouldCallCorrectConsumerDependingOnEventKey() throws Exception {

        Awaitility.await()
                .atMost(5, TimeUnit.SECONDS)
                .until(() -> allReceivedEventKeys.size(), is(4));

        Assert.assertThat(receivedBananaEventKeys, Matchers.contains("banana-1", "banana-2"));
        Assert.assertThat(receivedAppleEventKeys, Matchers.contains("apple-1", "apple-2"));
    }

    static class TestConfiguration {

        @EventSourceConsumer(
                name = "bananaConsumer",
                streamName = "test-stream",
                keyPattern = "^banana.*",
                payloadType = Map.class)
        public void consumeEventsWithEvenKey(Event<Map> event) {
            receivedBananaEventKeys.add(event.key());
            allReceivedEventKeys.add(event.key());
        }

        @EventSourceConsumer(
                name = "appleConsumer",
                streamName = "test-stream",
                keyPattern = "^apple.*",
                payloadType = Map.class)
        public void consumeEventsWithOddKey(Event<Map> event) {
            receivedAppleEventKeys.add(event.key());
            allReceivedEventKeys.add(event.key());
        }

        @Bean
        @Primary
        public SnapshotReadService snapshotReadService() {
            File file = new File(getClass().getClassLoader().getResource("apple-banana-snapshot-2017-11-27T09-02Z-3053797267191232636.json.zip").getFile());

            SnapshotReadService snapshotReadServiceMock = Mockito.mock(SnapshotReadService.class);
            when(snapshotReadServiceMock.downloadLatestSnapshot(any())).thenReturn(Optional.of(file));
            return snapshotReadServiceMock;
        }

        @Bean
        @Primary
        public KinesisClient kinesisClient() {
            return new KinesisClient() {
                @Override
                public void close() {
                    // do nothing
                }
                @Override
                public DescribeStreamResponse describeStream(DescribeStreamRequest describeStreamRequest) {
                    throw new UnsupportedOperationException("test kinesis client that throws exception on purpose");
                }
            };
        }

    }

}

