package de.otto.synapse.compaction.s3;

import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.consumer.MessageDispatcher;
import de.otto.synapse.state.ConcurrentMapStateRepository;
import de.otto.synapse.state.StateRepository;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableMap.of;
import static de.otto.synapse.channel.ChannelPosition.channelPosition;
import static de.otto.synapse.channel.ChannelPosition.fromHorizon;
import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static de.otto.synapse.compaction.s3.SnapshotServiceTestUtils.snapshotProperties;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.StringStartsWith.startsWith;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SnapshotWriteServiceTest {

    private static final String STREAM_NAME = "teststream";

    private SnapshotWriteService testee;
    private S3Client s3Client;

    @Before
    public void setUp() {
        s3Client = mock(S3Client.class);
        testee = new SnapshotWriteService(s3Client, snapshotProperties());
    }

    @After
    public void tearDown() throws Exception {
        deleteSnapshotFilesFromTemp();
    }

    @Test
    public void shouldUploadSnapshotFile() throws Exception {
        StateRepository<String> stateRepository = new ConcurrentMapStateRepository<>("test");
        stateRepository.put("testKey", "{\"content\":\"testValue1\"}");
        when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenReturn(PutObjectResponse.builder().build());
        //when
        String fileName = testee.writeSnapshot(STREAM_NAME, fromHorizon(), stateRepository);
        File file = new File(System.getProperty("java.io.tmpdir") + "/" + fileName);

        //then
        Mockito.verify(s3Client).putObject(any(PutObjectRequest.class), any(RequestBody.class));

        assertThat(fileName, startsWith("compaction-" + STREAM_NAME + "-snapshot-"));
        assertFalse(file.exists());
    }

    @Test
    public void shouldCreateCorrectSnapshotFile() throws Exception {
        StateRepository<String> stateRepository = new ConcurrentMapStateRepository<>("test");
        stateRepository.put("testKey", "{\"testValue1\": \"value1\"}");
        stateRepository.put("testKey2", "{\"testValue2\": \"value2\"}");

        //when
        ChannelPosition channelPosition = channelPosition(
                fromPosition("shard1", "1234"),
                fromPosition("shard2", "abcde"));
        File snapshot = testee.createSnapshot(STREAM_NAME, channelPosition, stateRepository);

        //then
        Map<String, Map> data = new HashMap<>();


        final MessageConsumer<Map> messageConsumer = MessageConsumer.of(".*", Map.class,
                (event) -> data.put(event.getKey().compactionKey(), event.getPayload()));
        ChannelPosition actualChannelPosition = new SnapshotParser().parse(
                snapshot,
                new MessageDispatcher(singletonList(messageConsumer)));

        assertThat(actualChannelPosition, is(channelPosition));
        assertThat(data.get("testKey"), is(of("testValue1", "value1")));
        assertThat(data.get("testKey2"), is(of("testValue2", "value2")));
        assertThat(data.size(), is(2));
    }

    @Test
    public void shouldDeleteSnapshotEvenIfUploadFails() throws Exception {
        // given
//        doThrow(new RuntimeException("forced test exception")).when(s3Helper).upload(any(), any());
        StateRepository<String> stateRepository = new ConcurrentMapStateRepository<>("test");
        stateRepository.put("testKey", "testValue1");
        stateRepository.put("testKey2", "testValue2");

        ChannelPosition channelPosition = channelPosition(fromPosition("shard1", "1234"), fromPosition("shard2", "abcde"));

        // when
        try {
            testee.writeSnapshot(STREAM_NAME, channelPosition, stateRepository);
        } catch (RuntimeException e) {
            // ignore exception
        }

        // then
        assertThat(getSnapshotFilePaths().size(), is(0));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldDeleteSnapshotWhenCreatingSnapshotFails() throws Exception {
        // given
        StateRepository<String> stateRepository = mock(StateRepository.class);
        when(stateRepository.get(any())).thenThrow(new RuntimeException("forced test exception"));

        ChannelPosition channelPosition = channelPosition(fromPosition("shard1", "1234"), fromPosition("shard2", "abcde"));

        // when
        try {
            testee.writeSnapshot(STREAM_NAME, channelPosition, stateRepository);
        } catch (RuntimeException e) {
            // ignore exception
        }

        // then
        assertThat(getSnapshotFilePaths().size(), is(0));
    }

    private void deleteSnapshotFilesFromTemp() throws IOException {
        getSnapshotFilePaths()
                .forEach(path -> {
                    try {
                        Files.deleteIfExists(path);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    private List<Path> getSnapshotFilePaths() throws IOException {
        return Files.list(Paths.get(System.getProperty("java.io.tmpdir")))
                .filter(p -> p.toString().startsWith("/tmp/compaction-teststream-snapshot-"))
                .collect(Collectors.toList());
    }

}
