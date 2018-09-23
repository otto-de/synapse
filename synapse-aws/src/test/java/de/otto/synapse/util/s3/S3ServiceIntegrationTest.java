package de.otto.synapse.util.s3;

import de.otto.synapse.configuration.aws.AwsConfiguration;
import de.otto.synapse.configuration.aws.S3TestConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.File;
import java.io.FileWriter;
import java.util.List;

import static java.nio.file.Files.createTempFile;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {S3TestConfiguration.class, AwsConfiguration.class})
@ActiveProfiles("test")
public class S3ServiceIntegrationTest {

    private static final String TESTBUCKET = "testbucket";
    @Autowired
    private S3Client s3Client;
    private S3Service s3Service;

    @Before
    public void setUp() {
        s3Service = new S3Service(s3Client);
        s3Service.createBucket(TESTBUCKET);
    }

    @After
    public void tearDown() {
        s3Service.deleteAllObjectsInBucket(TESTBUCKET);
    }

    @Test
    public void shouldOnlyDeleteFilesWithPrefix() throws Exception {
        // given
        s3Service.upload(TESTBUCKET, createTestfile("test", ".txt", "Hello World!"));
        s3Service.upload(TESTBUCKET, createTestfile("prefix", ".txt", "Hello World!"));

        // when
        s3Service.deleteAllObjectsWithPrefixInBucket(TESTBUCKET, "prefix");

        // then
        final List<String> allFiles = s3Service.listAllFiles(TESTBUCKET);
        assertThat(allFiles, contains(startsWith("test")));
        assertThat(allFiles, not(contains(startsWith("prefixed_test"))));
    }

    @Test
    public void shouldDeleteAllFilesInBucket() throws Exception {
        //given
        s3Service.upload(TESTBUCKET, createTempFile("test", ".json.zip").toFile());
        s3Service.upload(TESTBUCKET, createTempFile("prefixed_test", ".json.zip").toFile());

        //when
        s3Service.deleteAllObjectsInBucket(TESTBUCKET);

        //then
        final List<String> allFiles = s3Service.listAllFiles(TESTBUCKET);
        assertThat(allFiles, hasSize(0));
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
