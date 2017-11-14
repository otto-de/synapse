package de.otto.edison.eventsourcing.s3.local;

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.sync.RequestBody;

import java.io.ByteArrayInputStream;

import static com.google.common.base.Charsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class LocalS3ClientTest {

    private LocalS3Client testee;

    @Before
    public void setUp() throws Exception {
        testee = new LocalS3Client();
//        testee.createBucket("someBucket");
    }

    @Test
    public void shouldListObjectsInBucket() throws Exception {
        // given
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket("someBucket")
                .key("someObject")
                .build();
        RequestBody requestBody = createRequestBody("content");
        testee.putObject(putObjectRequest, requestBody);
//
//        // when
        ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder()
                .bucket("someBucket")
                .build();
        ListObjectsV2Response listObjectsV2Response = testee.listObjectsV2(listObjectsV2Request);

        //then
        assertThat(listObjectsV2Response.contents().size(), is(1));
        assertThat(listObjectsV2Response.contents().get(0).key(), is("someObject"));
    }

    private RequestBody createRequestBody(String content) {
        return RequestBody.of(new ByteArrayInputStream(content.getBytes(UTF_8)), content.length());
    }
}
