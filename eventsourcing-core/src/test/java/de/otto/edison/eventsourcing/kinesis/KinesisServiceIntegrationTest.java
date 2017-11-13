package de.otto.edison.eventsourcing.kinesis;


import de.otto.edison.eventsourcing.consumer.Event;
import de.otto.edison.eventsourcing.consumer.StreamPosition;
import de.otto.edison.eventsourcing.kinesis.testsupport.TestStreamSource;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import software.amazon.awssdk.services.kinesis.KinesisClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.synchronizedList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;


@RunWith(SpringRunner.class)
@ActiveProfiles("test")
@EnableAutoConfiguration
@ComponentScan(basePackages = {"de.otto.edison.eventsourcing"})
@SpringBootTest(classes = KinesisServiceIntegrationTest.class)
public class KinesisServiceIntegrationTest {

    private static final int EXPECTED_NUMBER_OF_ENTRIES_IN_FIRST_SET = 10;
    private static final int EXPECTED_NUMBER_OF_ENTRIES_IN_SECOND_SET = 10;
    private static final int EXPECTED_NUMBER_OF_SHARDS = 2;
    private static final String STREAM_NAME = "promo-compaction-test";

    @Autowired
    private KinesisClient kinesisClient;

    private KinesisStream kinesisStream;

    @Before
    public void setUp() throws Exception {
        kinesisStream = new KinesisStream(kinesisClient, STREAM_NAME);
    }

    @Test
    public void consumeDataFromKinesisStream() throws Exception {
        // when
        StreamPosition startSequenceNumberPerShard = writeToStream("users_small1.txt").getFirstReadPosition();

        // then
        List<Event<String>> events = synchronizedList(new ArrayList<Event<String>>());
        StreamPosition streamPosition = new KinesisEventSource<>(Function.identity(), kinesisStream)
                .consumeAll(startSequenceNumberPerShard, stopWhenEmpty(), events::add);

        assertThat(events, not(empty()));
        assertThat(events, hasSize(EXPECTED_NUMBER_OF_ENTRIES_IN_FIRST_SET));
        assertThat(streamPosition.shards(), hasSize(EXPECTED_NUMBER_OF_SHARDS));
    }

    @Test
    public void consumerShouldRereadNoMoreAfterStartingPoint() {
        // when
        writeToStream("users_small1.txt").getFirstReadPosition();
        StreamPosition startStreamPosition = writeToStream("users_small2.txt").getFirstReadPosition();

        // then
        List<Event<String>> events = synchronizedList(new ArrayList<Event<String>>());
        StreamPosition nextStreamPosition = new KinesisEventSource<>(Function.identity(), kinesisStream)
                .consumeAll(startStreamPosition, stopWhenEmpty(), events::add);

        assertThat(events, not(empty()));
        assertThat(nextStreamPosition.shards(), hasSize(EXPECTED_NUMBER_OF_SHARDS));
        assertThat(events, hasSize(EXPECTED_NUMBER_OF_ENTRIES_IN_SECOND_SET));
        assertThat(events.stream().map(Event::key).sorted().collect(Collectors.toList()), is(expectedListOfKeys()));
    }

    @Test
    public void consumerShouldResumeAtStartingPoint() {
        // when
        writeToStream("users_small1.txt");
        StreamPosition lastSequenceNumberPerShard2 = writeToStream("users_small2.txt").getLastStreamPosition();

        // then
        List<Event<String>> events = synchronizedList(new ArrayList<Event<String>>());
        StreamPosition lastSequenceNumberPerShard3 = new KinesisEventSource<>(Function.identity(), kinesisStream)
                .consumeAll(lastSequenceNumberPerShard2, stopWhenEmpty(), events::add);

        assertThat(events, empty());
        assertThat(lastSequenceNumberPerShard3.shards(), hasSize(EXPECTED_NUMBER_OF_SHARDS));
    }

    private TestStreamSource writeToStream(String filename) {
        TestStreamSource streamSource = new TestStreamSource(kinesisClient, STREAM_NAME, filename);
        streamSource.writeToStream();
        return streamSource;
    }

    private List<String> expectedListOfKeys() {
        return IntStream.range(EXPECTED_NUMBER_OF_ENTRIES_IN_FIRST_SET + 1, EXPECTED_NUMBER_OF_ENTRIES_IN_FIRST_SET + EXPECTED_NUMBER_OF_ENTRIES_IN_SECOND_SET + 1).mapToObj(String::valueOf).collect(Collectors.toList());
    }

    private Predicate<Event<String>> stopWhenEmpty() {
        return Objects::isNull;
    }


}
