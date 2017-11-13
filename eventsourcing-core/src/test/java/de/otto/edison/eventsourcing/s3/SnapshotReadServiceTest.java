package de.otto.edison.eventsourcing.s3;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.aws.s3.S3Service;
import de.otto.edison.eventsourcing.configuration.EventSourcingProperties;
import de.otto.edison.eventsourcing.consumer.StreamPosition;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.File;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class SnapshotReadServiceTest {

    private SnapshotReadService testee;
    private S3Service s3Service;

    @Before
    public void setUp() throws Exception {
        EventSourcingProperties eventSourcingProperties = createEventSourcingProperties();
        s3Service = mock(S3Service.class);
        testee = new SnapshotReadService(s3Service, eventSourcingProperties, new ObjectMapper());
    }

    @Test
    public void shouldProcessSnapshotFile() throws Exception {
        //given
        File file = new File(getClass().getClassLoader().getResource("compaction-integrationtest-snapshot-2017-09-29T09-02Z-3053797267191232636.json.zip").getFile());
        Map<String, String> allData = new HashMap<>();
        //when
        final StreamPosition shardPositions = testee.processSnapshotFile(
                file,
                "test",
                (x) -> false,
                (event) -> allData.put(event.key(), event.payload()),
                String.class);
        //then
        assertThat(shardPositions.shards().size(), is(2));
        assertThat(shardPositions.positionOf("shardId-000000000000"), is("0"));
        assertThat(shardPositions.positionOf("shardId-000000000001"), is("0"));
        assertThat(allData.size(), is(5000));
        assertThat(allData.get("401"), is("{\"userid\":401,\"username\":\"FRE90ZMX\",\"firstname\":\"Kermit\",\"lastname\":\"Morris\",\"city\":\"Lawrenceville\",\"state\":\"YT\",\"email\":\"in.molestie.tortor@sodalesnisimagna.ca\",\"phone\":\"(812) 221-1857\",\"likesports\":false,\"liketheatre\":false,\"likeconcerts\":true,\"likejazz\":false,\"likeclassical\":false,\"likeopera\":false,\"likerock\":false,\"likevegas\":false,\"likebroadway\":false,\"likemusicals\":false}"));
        assertThat(allData.get("4674"), is("{\"userid\":4674,\"username\":\"WCV49LUY\",\"firstname\":\"Linda\",\"lastname\":\"Ramirez\",\"city\":\"Bristol\",\"state\":\"MA\",\"email\":\"egestas@vitae.com\",\"phone\":\"(484) 151-5993\",\"likesports\":false,\"liketheatre\":false,\"likeconcerts\":true,\"likejazz\":false,\"likeclassical\":false,\"likeopera\":false,\"likerock\":false,\"likevegas\":false,\"likebroadway\":false,\"likemusicals\":false}"));
    }

    @Test
    public void shouldDownloadLatestSnapshotFileFromBucket() throws Exception {
        //given
        final S3Object obj1 = mock(S3Object.class);
        when(obj1.key()).thenReturn("compaction-test-snapshot-1.json.zip");
        when(obj1.lastModified()).thenReturn(Instant.MIN);
        final S3Object obj2 = mock(S3Object.class);
        when(obj2.key()).thenReturn("compaction-test-snapshot-2.json.zip");
        when(obj2.lastModified()).thenReturn(Instant.MAX);
        when(s3Service.listAll("testBucket")).thenReturn(asList(obj1, obj2));

        //when
        Optional<S3Object> s3Object = testee.getLatestZip("testBucket", "test");

        //then
        assertThat(s3Object.get().key(), is("compaction-test-snapshot-2.json.zip"));
    }

    @Test
    public void shouldReturnOptionalEmptyWhenNoFileInBucket() throws Exception {
        //when
        when(s3Service.listAll(anyString())).thenReturn(emptyList());
        Optional<S3Object> s3Object = testee.getLatestZip("testBucket", "DOES_NOT_EXIST");

        //then
        assertThat(s3Object.isPresent(), is(false));
    }

    private EventSourcingProperties createEventSourcingProperties() {
        EventSourcingProperties eventSourcingProperties = new EventSourcingProperties();
        EventSourcingProperties.Snapshot snapshot = new EventSourcingProperties.Snapshot();
        snapshot.setBucketTemplate("test<stream-name>");
        eventSourcingProperties.setSnapshot(snapshot);
        return eventSourcingProperties;
    }
}
