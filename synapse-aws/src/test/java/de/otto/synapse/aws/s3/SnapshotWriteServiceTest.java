package de.otto.synapse.aws.s3;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.aws.s3.S3Service;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.consumer.MessageDispatcher;
import de.otto.synapse.state.ConcurrentHashMapStateRepository;
import de.otto.synapse.state.StateRepository;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

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
import static de.otto.synapse.aws.s3.SnapshotServiceTestUtils.snapshotProperties;
import static de.otto.synapse.channel.ChannelPosition.channelPosition;
import static de.otto.synapse.channel.ChannelPosition.fromHorizon;
import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.StringStartsWith.startsWith;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class SnapshotWriteServiceTest {

    private static final String STREAM_NAME = "teststream";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private SnapshotWriteService testee;
    private S3Service s3Service;

    @Before
    public void setUp() {
        s3Service = mock(S3Service.class);
        testee = new SnapshotWriteService(s3Service, snapshotProperties());
    }

    @After
    public void tearDown() throws Exception {
        deleteSnapshotFilesFromTemp();
    }

    @Test
    public void shouldUploadSnapshotFile() throws Exception {
        StateRepository<String> stateRepository = new ConcurrentHashMapStateRepository<>();
        stateRepository.put("testKey", "{\"content\":\"testValue1\"}");

        //when
        String fileName = testee.writeSnapshot(STREAM_NAME, fromHorizon(), stateRepository);

        //then
        ArgumentCaptor<File> fileArgumentCaptor = ArgumentCaptor.forClass(File.class);
        Mockito.verify(s3Service).upload(eq("test-" + STREAM_NAME), fileArgumentCaptor.capture());

        File actualFile = fileArgumentCaptor.getValue();
        assertThat(actualFile.getName(), containsString(fileName));
        assertThat(fileName, startsWith("compaction-" + STREAM_NAME + "-snapshot-"));

        assertFalse(actualFile.exists());
    }

    @Test
    public void shouldCreateCorrectSnapshotFile() throws Exception {
        StateRepository<String> stateRepository = new ConcurrentHashMapStateRepository<>();
        stateRepository.put("testKey", "{\"testValue1\": \"value1\"}");
        stateRepository.put("testKey2", "{\"testValue2\": \"value2\"}");

        //when
        ChannelPosition channelPosition = channelPosition(fromPosition("shard1", "1234"), fromPosition("shard2", "abcde"));
        File snapshot = testee.createSnapshot(STREAM_NAME, channelPosition, stateRepository);

        //then
        Map<String, Map> data = new HashMap<>();


        final MessageConsumer<Map> messageConsumer = MessageConsumer.of(".*", Map.class,
                (event) -> data.put(event.getKey(), event.getPayload()));

        ChannelPosition actualChannelPosition = new SnapshotConsumerService().consumeSnapshot(snapshot,
                STREAM_NAME,
                (event) -> false,
                new MessageDispatcher(OBJECT_MAPPER, singletonList(messageConsumer)));

        assertThat(actualChannelPosition, is(channelPosition));
        assertThat(data.get("testKey"), is(of("testValue1", "value1")));
        assertThat(data.get("testKey2"), is(of("testValue2", "value2")));
        assertThat(data.size(), is(2));
    }

    @Test
    public void shouldDeleteSnapshotEvenIfUploadFails() throws Exception {
        // given
        doThrow(new RuntimeException("forced test exception")).when(s3Service).upload(any(), any());
        StateRepository<String> stateRepository = new ConcurrentHashMapStateRepository<>();
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
