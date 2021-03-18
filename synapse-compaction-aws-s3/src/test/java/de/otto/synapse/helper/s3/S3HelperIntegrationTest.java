package de.otto.synapse.helper.s3;

import de.otto.synapse.configuration.aws.S3TestConfiguration;
import de.otto.synapse.configuration.aws.SynapseAwsAuthConfiguration;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;

import static java.nio.file.Files.createTempFile;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {S3TestConfiguration.class, SynapseAwsAuthConfiguration.class})
@ActiveProfiles("test")
public class S3HelperIntegrationTest {

    private static final String TESTBUCKET = "testbucket";

    private S3Helper s3Helper;
    private Path tempDir;

    @Before
    public void setUp() throws URISyntaxException, IOException {
        S3Client s3Client = S3Client.builder()
                .endpointOverride(new URI("http://localhost:4566"))
                .overrideConfiguration(ClientOverrideConfiguration.builder().build())
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("foobar", "foobar")))
                .region(Region.US_EAST_1).build();
        s3Helper = new S3Helper(s3Client);
        s3Helper.createBucket(TESTBUCKET);
        tempDir = Files.createDirectories(Paths.get(System.getProperty("java.io.tmpdir"), "synapse-test-" + UUID.randomUUID().toString()));
    }

    @After
    public void tearDown() throws IOException {
        s3Helper.deleteAllObjectsInBucket(TESTBUCKET);
        FileUtils.deleteDirectory(tempDir.toFile());
    }

    @Test
    public void shouldOnlyDeleteFilesWithPrefix() throws Exception {
        // given
        s3Helper.upload(TESTBUCKET, createTestfile("test", ".txt", "Hello World!"));
        s3Helper.upload(TESTBUCKET, createTestfile("prefix", ".txt", "Hello World!"));

        // when
        s3Helper.deleteAllObjectsWithPrefixInBucket(TESTBUCKET, "prefix");

        // then
        final List<String> allFiles = s3Helper.listAllFiles(TESTBUCKET);
        assertThat(allFiles, contains(startsWith("test")));
        assertThat(allFiles, not(contains(startsWith("prefixed_test"))));
    }

    @Test
    public void shouldDeleteAllFilesInBucket() throws Exception {
        //given
        s3Helper.upload(TESTBUCKET, createTempFile("test", ".json.zip").toFile());
        s3Helper.upload(TESTBUCKET, createTempFile("prefixed_test", ".json.zip").toFile());

        //when
        s3Helper.deleteAllObjectsInBucket(TESTBUCKET);

        //then
        final List<String> allFiles = s3Helper.listAllFiles(TESTBUCKET);
        assertThat(allFiles, hasSize(0));
    }

    @Test
    public void fileThatWasUploadedWithMultipartShouldBeTheSameAfterDownloadingItAgain() throws Exception {

        //given
        File fileToUpload = Paths.get(tempDir.toString(), "fileToUpload").toFile();
        File downloadedFile = Paths.get(tempDir.toString(), "downloadedFile").toFile();
        RandomAccessFile raf = new RandomAccessFile(fileToUpload, "rw");
        raf.setLength(22 * 1024 * 1024); //22 MB
        raf.close();

        //when
        s3Helper.uploadAsMultipart(TESTBUCKET, fileToUpload, 5 * 1024 * 1024);
        s3Helper.download(TESTBUCKET, "fileToUpload", downloadedFile.toPath());

        //then
        assertThat(FileUtils.contentEquals(fileToUpload, downloadedFile), is(true));
    }

    private File createTestfile(final String prefix, final String suffix, final String content) throws Exception {
        final File tempFile = createTempFile(prefix, suffix).toFile();
        try (final FileWriter writer = new FileWriter(tempFile)) {
            writer.append(content);
            writer.flush();
        }
        return tempFile;
    }

}
