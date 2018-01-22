package de.otto.edison.eventsourcing.s3;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import de.otto.edison.aws.s3.S3Service;
import de.otto.edison.eventsourcing.configuration.EventSourcingProperties;
import de.otto.edison.eventsourcing.consumer.EventConsumer;
import de.otto.edison.eventsourcing.consumer.EventConsumers;
import de.otto.edison.eventsourcing.consumer.StreamPosition;
import de.otto.edison.eventsourcing.encryption.Base64EncodingTextEncryptor;
import de.otto.edison.eventsourcing.state.DefaultStateRepository;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.security.crypto.codec.Hex;
import org.springframework.security.crypto.encrypt.Encryptors;
import org.springframework.security.crypto.encrypt.TextEncryptor;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.StringStartsWith.startsWith;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SnapshotWriteServiceTest {

    private static final String STREAM_NAME = "teststream";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private SnapshotWriteService testee;
    private S3Service s3Service;
    private TextEncryptor encryptor;
    private S3Client s3Client;

    @Before
    public void setUp() {
        EventSourcingProperties eventSourcingProperties = SnapshotServiceTestUtils.createEventSourcingProperties();
        s3Service = mock(S3Service.class);
        s3Client = mock(S3Client.class);
        encryptor = new Base64EncodingTextEncryptor(Encryptors.standard("test", new String(Hex.encode("test".getBytes()))));
        testee = new SnapshotWriteService(s3Service, eventSourcingProperties, encryptor, s3Client);
    }

    @After
    public void tearDown() throws Exception {
        deleteSnapshotFilesFromTemp();
    }

    @Test
    public void shouldUploadSnapshotFile() throws Exception {
        DefaultStateRepository<String> stateRepository = new DefaultStateRepository<>();
        stateRepository.put("testKey", "testValue1");

        //when
        String fileName = testee.takeSnapshot(STREAM_NAME, StreamPosition.of(), stateRepository);

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
        DefaultStateRepository<String> stateRepository = new DefaultStateRepository<>();
        stateRepository.put("testKey", "{\"testValue1\": \"value1\"}");
        stateRepository.put("testKey2", "{\"testValue2\": \"value2\"}");
//        stateRepository.put("testKey2", "testValue2");

        //when
        StreamPosition streamPosition = StreamPosition.of(ImmutableMap.of("shard1", "1234", "shard2", "abcde"));
        File snapshot = testee.createSnapshot(STREAM_NAME, streamPosition, stateRepository);

        //then
        Map<String, Map> data = new HashMap<>();

        SnapshotConsumerService snapshotConsumerService = new SnapshotConsumerService(Encryptors.noOpText());

        final EventConsumer<Map> eventConsumer = EventConsumer.of(".*", Map.class,
                (event) -> {
                    System.out.println(event);
                    data.put(event.key(), event.payload());
                });
        StreamPosition actualStreamPosition = snapshotConsumerService.consumeSnapshot(snapshot,
                "test",
                (event) -> false,
                new EventConsumers(OBJECT_MAPPER, singletonList(eventConsumer)));

        assertThat(actualStreamPosition, is(streamPosition));
        assertThat(data.get("testKey"), is(ImmutableMap.of("testValue1", "value1")));
        assertThat(data.get("testKey2"), is(ImmutableMap.of("testValue2", "value2")));
        assertThat(data.size(), is(2));
    }

    @Test
    public void shouldDeleteSnapshotEvenIfUploadFails() throws Exception {
        // given
        doThrow(new RuntimeException("forced test exception")).when(s3Service).upload(any(), any());
        DefaultStateRepository<String> stateRepository = new DefaultStateRepository<>();
        stateRepository.put("testKey", "testValue1");
        stateRepository.put("testKey2", "testValue2");

        StreamPosition streamPosition = StreamPosition.of(ImmutableMap.of("shard1", "1234", "shard2", "abcde"));

        // when
        try {
            testee.takeSnapshot(STREAM_NAME, streamPosition, stateRepository);
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
        DefaultStateRepository<String> stateRepository = mock(DefaultStateRepository.class);
        when(stateRepository.get(any())).thenThrow(new RuntimeException("forced test exception"));

        StreamPosition streamPosition = StreamPosition.of(ImmutableMap.of("shard1", "1234", "shard2", "abcde"));

        // when
        try {
            testee.takeSnapshot(STREAM_NAME, streamPosition, stateRepository);
        } catch (RuntimeException e) {
            // ignore exception
        }

        // then
        assertThat(getSnapshotFilePaths().size(), is(0));
    }

    @Test
    public void shouldNotDecryptBecauseNotEncrypted() {
        String entry = "{\"id\": \"someId\", \"otherDings\": \"bla\" }";

        String out = testee.decryptIfNecessary(entry);

        assertThat(out, is(entry));
    }

    @Test
    public void shouldDecryptBecauseEncrypted() {
        String entry = "{\"id\": \"someId\", \"otherDings\": \"bla\" }";
        String encryptedEntry = encryptor.encrypt(entry);

        String out = testee.decryptIfNecessary(encryptedEntry);

        assertThat(out, is(entry));
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
