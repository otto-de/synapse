package de.otto.edison.eventsourcing.s3;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.eventsourcing.consumer.StreamPosition;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class SnapshotConsumerServiceTest {

    private SnapshotConsumerService testee;

    @Before
    public void setUp() throws Exception {
        testee = new SnapshotConsumerService(new ObjectMapper());
    }

    @Test
    public void shouldConsumeSnapshotFile() throws Exception {
        //given
        File file = new File(getClass().getClassLoader().getResource("compaction-integrationtest-snapshot-2017-09-29T09-02Z-3053797267191232636.json.zip").getFile());
        Map<String, String> allData = new HashMap<>();
        //when
        final StreamPosition shardPositions = testee.consumeSnapshot(
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


}