package de.otto.edison.eventsourcing.s3.local;

import software.amazon.awssdk.SdkBaseException;
import software.amazon.awssdk.SdkClientException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.sync.RequestBody;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.io.ByteStreams.toByteArray;
import static de.otto.edison.eventsourcing.s3.local.BucketItem.bucketItemBuilder;

public class LocalS3Client implements S3Client {

    private static final Instant BUCKET_DEFAULT_CREATION_DATE = Instant.parse("2017-01-01T10:00:00.00Z");

    private Map<String, Map<String,BucketItem>> bucketsWithContents;

    public LocalS3Client() {
        this.bucketsWithContents = new HashMap<>();
    }

    @Override
    public ListObjectsV2Response listObjectsV2(ListObjectsV2Request listObjectsV2Request) throws NoSuchBucketException, SdkBaseException, SdkClientException, S3Exception {
        Collection<S3Object> s3Objects = bucketsWithContents.get(listObjectsV2Request.bucket())
                .values()
                .stream()
                .map(bucketItem -> S3Object.builder()
                        .key(bucketItem.getName())
                        .lastModified(bucketItem.getLastModified())
                        .build())
                .collect(Collectors.toList());

        return ListObjectsV2Response.builder()
                .contents(s3Objects)
                .keyCount(s3Objects.size())
                .build();
    }

    @Override
    public CreateBucketResponse createBucket(CreateBucketRequest createBucketRequest) throws BucketAlreadyExistsException, BucketAlreadyOwnedByYouException, SdkBaseException, SdkClientException, S3Exception {
        bucketsWithContents.put(createBucketRequest.bucket(), new HashMap<>());
        return CreateBucketResponse.builder().build();
    }

    @Override
    public PutObjectResponse putObject(PutObjectRequest putObjectRequest, RequestBody requestBody) throws SdkBaseException, SdkClientException, S3Exception {
        try {
            bucketsWithContents.get(putObjectRequest.bucket()).put(putObjectRequest.key(),
                    bucketItemBuilder()
                    .withName(putObjectRequest.key())
                    .withData(toByteArray(requestBody.asStream()))
                    .withLastModifiedNow()
                    .build());
            return PutObjectResponse.builder().build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public DeleteObjectsResponse deleteObjects(DeleteObjectsRequest deleteObjectsRequest) throws SdkBaseException, SdkClientException, S3Exception {
        Map<String, BucketItem> bucketItemMap = bucketsWithContents.get(deleteObjectsRequest.bucket());
        deleteObjectsRequest.delete().objects()
                .stream()
                .map(ObjectIdentifier::key)
                .forEach(bucketItemMap::remove);
        return DeleteObjectsResponse.builder().build();
    }

    @Override
    public ListBucketsResponse listBuckets(ListBucketsRequest listBucketsRequest) throws SdkBaseException, SdkClientException, S3Exception {
        return ListBucketsResponse.builder()
                .buckets(bucketsWithContents.keySet().stream()
                        .map(name -> Bucket.builder()
                                .creationDate(BUCKET_DEFAULT_CREATION_DATE)
                                .name(name)
                                .build())
                        .collect(Collectors.toList())).build();
    }

    @Override
    public void close() {
        System.out.println("s3 closing...");
    }
}
