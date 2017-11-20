package de.otto.edison.eventsourcing.s3.local;

import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.core.sync.RequestBody;

import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;

import static com.google.common.base.Charsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;

public class LocalS3ClientTest {

    private LocalS3Client testee;

    @Before
    public void setUp() throws Exception {
        testee = new LocalS3Client();
        testee.createBucket(CreateBucketRequest.builder().bucket("someBucket").build());
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

    @Test
    public void deleteShouldRemoveItemsFromBucket() throws Exception {
        // given
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket("someBucket")
                .key("someObject")
                .build();
        RequestBody requestBody = createRequestBody("content");
        testee.putObject(putObjectRequest, requestBody);
        testee.deleteObjects(DeleteObjectsRequest.builder().bucket("someBucket").delete(Delete.builder().objects
                (ObjectIdentifier.builder().key("someObject").build()).build()).build());

        // when
        ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder()
                .bucket("someBucket")
                .build();
        ListObjectsV2Response listObjectsV2Response = testee.listObjectsV2(listObjectsV2Request);

        //then
        assertThat(listObjectsV2Response.contents().size(), is(0));
    }

    @Test
    public void listBucketsShouldReturnSingleBucket() throws Exception {
        assertEquals(testee.listBuckets()
                .buckets()
                .stream()
                .map(Bucket::name)
                .collect(Collectors.toList()),
                Collections.singletonList("someBucket"));
    }

    @Test
    public void listBucketsShouldReturnsSecondBucketSingleBucket() throws Exception {
        // when
        testee.createBucket(CreateBucketRequest.builder().bucket("newBucket").build());

        // then
        assertEquals(testee.listBuckets()
                .buckets()
                .stream()
                .map(Bucket::name)
                .sorted()
                .collect(Collectors.toList()), Arrays.asList("newBucket", "someBucket"));
    }

    private RequestBody createRequestBody(String content) {
        return RequestBody.of(new ByteArrayInputStream(content.getBytes(UTF_8)), content.length());
    }
}
