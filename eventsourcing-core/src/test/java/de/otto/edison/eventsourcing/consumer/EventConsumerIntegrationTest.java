package de.otto.edison.eventsourcing.consumer;

import de.otto.edison.eventsourcing.annotation.EventSourceConsumer;
import de.otto.edison.eventsourcing.configuration.EventSourcingBootstrapConfiguration;
import de.otto.edison.eventsourcing.configuration.EventSourcingConfiguration;
import de.otto.edison.eventsourcing.configuration.SnapshotConfiguration;
import de.otto.edison.eventsourcing.s3.SnapshotReadService;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.Ignore;
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
    static List<Apple> receivedAppleEventPayloads = new ArrayList<>();
    static List<Banana> receivedBananaEventPayloads = new ArrayList<>();

    @Ignore // make this test work
    @Test
    public void shouldCallCorrectConsumerDependingOnEventKey() throws Exception {

        Awaitility.await()
                .atMost(5, TimeUnit.SECONDS)
                .until(() -> allReceivedEventKeys.size(), is(4));

        Assert.assertThat(receivedBananaEventPayloads.size(), is(2));
        Assert.assertThat(receivedBananaEventPayloads.get(0).bananaId, is("banana-1"));
        Assert.assertThat(receivedBananaEventPayloads.get(0).bananaId, is("banana-2"));
        Assert.assertThat(receivedAppleEventPayloads.size(), is(2));
        Assert.assertThat(receivedAppleEventPayloads.get(0).appleId, is("apple-1"));
        Assert.assertThat(receivedAppleEventPayloads.get(0).appleId, is("apple-2"));
    }

    private static class Apple {
        String appleId;
        String name;
    }

    private static class Banana {
        String bananaId;
        String name;
    }

    static class TestConfiguration {

        @EventSourceConsumer(
                name = "bananaConsumer",
                streamName = "test-stream",
                keyPattern = "^banana.*",
                payloadType = Banana.class)
        public void consumeEventsWithEvenKey(Event<Banana> event) {
            receivedBananaEventPayloads.add(event.payload());
            allReceivedEventKeys.add(event.key());
        }

        @EventSourceConsumer(
                name = "appleConsumer",
                streamName = "test-stream",
                keyPattern = "^apple.*",
                payloadType = Apple.class)
        public void consumeEventsWithOddKey(Event<Apple> event) {
            receivedAppleEventPayloads.add(event.payload());
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

