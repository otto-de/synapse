package de.otto.edison.eventsourcing.s3;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import de.otto.edison.eventsourcing.consumer.EventConsumer;
import de.otto.edison.eventsourcing.consumer.EventConsumers;
import de.otto.edison.eventsourcing.consumer.StreamPosition;
import org.junit.Before;
import org.junit.Test;
import org.springframework.security.crypto.encrypt.Encryptors;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class SnapshotConsumerServiceTest {

    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private SnapshotConsumerService testee;

    @Before
    public void setUp() throws Exception {
        testee = new SnapshotConsumerService(Encryptors.noOpText());
    }

    @Test
    public void shouldConsumeSnapshotFile() throws Exception {
        //given
        File file = new File(getClass().getClassLoader().getResource("compaction-integrationtest-snapshot-2017-09-29T09-02Z-3053797267191232636.json.zip").getFile());
        Map<String, Map> allData = new HashMap<>();
        //when
        final EventConsumer<Map> eventConsumer = EventConsumer.of(".*", Map.class, (event) -> {
            System.out.println(event.payload());
            allData.put(event.key(), event.payload());
        });
        final StreamPosition shardPositions = testee.consumeSnapshot(
                file,
                "test",
                (event) -> false,
                new EventConsumers(OBJECT_MAPPER, Collections.singletonList(eventConsumer)));
        //then
        assertThat(shardPositions.shards().size(), is(2));
        assertThat(shardPositions.positionOf("shardId-000000000000"), is("0"));
        assertThat(shardPositions.positionOf("shardId-000000000001"), is("0"));
        assertThat(allData.size(), is(5000));
        ImmutableMap.Builder<Object, Object> builder = ImmutableMap.builder();
        builder.put("userid", 401);
        builder.put("username", "FRE90ZMX");
        builder.put("firstname", "Kermit");
        builder.put("lastname", "Morris");
        builder.put("city", "Lawrenceville");
        builder.put("state", "YT");
        builder.put("email", "in.molestie.tortor@sodalesnisimagna.ca");
        builder.put("phone", "(812) 221-1857");
        builder.put("likesports", false);
        builder.put("liketheatre", false);
        builder.put("likeconcerts", true);
        builder.put("likejazz", false);
        builder.put("likeclassical", false);
        builder.put("likeopera", false);
        builder.put("likerock", false);
        builder.put("likevegas", false);
        builder.put("likebroadway", false);
        builder.put("likemusicals", false);

        assertThat(allData.get("401"), is(builder.build()));
    }


}
