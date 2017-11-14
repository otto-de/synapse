package de.otto.edison.eventsourcing.s3.local;

import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.io.ByteStreams;
import software.amazon.awssdk.SdkBaseException;
import software.amazon.awssdk.SdkClientException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.sync.RequestBody;

import java.io.IOException;
import java.util.Collection;
import java.util.stream.Collectors;

import static com.google.common.io.ByteStreams.toByteArray;
import static de.otto.edison.eventsourcing.s3.local.BucketItem.bucketItemBuilder;

public class LocalS3Client implements S3Client {

    private Multimap<String, BucketItem> bucketsWithContents;


    public LocalS3Client() {
        this.bucketsWithContents = MultimapBuilder.hashKeys().arrayListValues().build();
    }

    @Override
    public ListObjectsV2Response listObjectsV2(ListObjectsV2Request listObjectsV2Request) throws NoSuchBucketException, SdkBaseException, SdkClientException, S3Exception {
        Collection<S3Object> s3Objects = bucketsWithContents.get(listObjectsV2Request.bucket())
                .stream()
                .map(BucketItem::getName)
                .map(name -> S3Object.builder().key(name).build())
                .collect(Collectors.toList());

        return ListObjectsV2Response.builder()
                .contents(s3Objects)
                .keyCount(s3Objects.size())
                .build();
    }

    @Override
    public PutObjectResponse putObject(PutObjectRequest putObjectRequest, RequestBody requestBody) throws SdkBaseException, SdkClientException, S3Exception {
        try {
            bucketsWithContents.put(putObjectRequest.bucket(),
                    bucketItemBuilder()
                    .withName(putObjectRequest.key())
                    .withData(toByteArray(requestBody.asStream()))
                    .build());
            return PutObjectResponse.builder().build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        System.out.println("s3 closing...");
    }
}
