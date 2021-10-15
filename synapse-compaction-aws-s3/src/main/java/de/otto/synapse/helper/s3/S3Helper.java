package de.otto.synapse.helper.s3;

import org.slf4j.Logger;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.stream.Collectors.toList;
import static org.slf4j.LoggerFactory.getLogger;
import static software.amazon.awssdk.services.s3.model.Delete.builder;

public class S3Helper {

    private static final Logger LOG = getLogger(S3Helper.class);
    private static final long AWS_S3_FILE_LIMIT_OF_5GB_IN_BYTES = 5L * 1024 * 1024 * 1024; //5 GB
    public static final int PART_SIZE_IN_BYTES = 1024 * 1024 * 100; //100 MB

    private final S3Client s3Client;

    public S3Helper(final S3Client s3Client) {
        this.s3Client = s3Client;
    }

    public void createBucket(final String bucketName) {
        if (!listBucketNames().contains(bucketName)) {
            s3Client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
        }
    }

    public List<String> listBucketNames() {
        return s3Client
                .listBuckets()
                .buckets()
                .stream()
                .map(Bucket::name)
                .collect(toList());
    }

    public void upload(final String bucketName,
                       final File file) {

        if (file.length() >= AWS_S3_FILE_LIMIT_OF_5GB_IN_BYTES) {
            uploadAsMultipart(bucketName, file, PART_SIZE_IN_BYTES);
            return;
        }

        try (FileInputStream fis = new FileInputStream(file)) {
            final PutObjectResponse putObjectResponse = s3Client.putObject(PutObjectRequest.builder()
                            .bucket(bucketName)
                            .key(file.getName())
                            .build(),
                    RequestBody.fromInputStream(fis, file.length()));
            LOG.debug("upload {} to bucket {}: {}", file.getName(), bucketName, putObjectResponse.toString());
        } catch (IOException e) {
            LOG.error("Error while uploading {} to bucket {}", file.getName(), bucketName, e);
        }
    }

    /**
     *
     * @param bucketName
     * @param file
     * @param partSizeInBytes The minimum file size for multipart file parts is 5 MB
     */
    public void uploadAsMultipart(final String bucketName, final File file, int partSizeInBytes) {

        String filename = file.getName();

        // First create a multipart upload and get upload id
        CreateMultipartUploadRequest createMultipartUploadRequest = CreateMultipartUploadRequest.builder()
                .bucket(bucketName).key(filename)
                .build();
        CreateMultipartUploadResponse response = s3Client.createMultipartUpload(createMultipartUploadRequest);
        String uploadId = response.uploadId();

        ArrayList<CompletedPart> parts = new ArrayList<>();
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
             FileChannel inputChannel = randomAccessFile.getChannel()) {
            ByteBuffer byteBuffer = ByteBuffer.allocate(partSizeInBytes);
            int partNumber = 0;
            while (inputChannel.read(byteBuffer) > 0) {
                byteBuffer.flip();
                partNumber++;
                // Upload all the different parts of the object
                UploadPartRequest uploadPartRequest = UploadPartRequest.builder().bucket(bucketName).key(filename)
                        .uploadId(uploadId)
                        .partNumber(partNumber).build();
                String etag = s3Client.uploadPart(uploadPartRequest, RequestBody.fromByteBuffer(byteBuffer)).eTag();
                CompletedPart part = CompletedPart.builder().partNumber(partNumber).eTag(etag).build();
                parts.add(part);
            }

            // Finally call completeMultipartUpload operation to tell S3 to merge all uploaded
            // parts and finish the multipart operation.
            CompletedMultipartUpload completedMultipartUpload = CompletedMultipartUpload.builder().parts(parts).build();
            CompleteMultipartUploadRequest completeMultipartUploadRequest =
                    CompleteMultipartUploadRequest.builder().bucket(bucketName).key(filename).uploadId(uploadId)
                            .multipartUpload(completedMultipartUpload).build();
            s3Client.completeMultipartUpload(completeMultipartUploadRequest);
        } catch (IOException e) {
            LOG.error("Something went wrong during multipart upload!!!", e);
        }
    }

    public boolean download(final String bucketName,
                            final String fileName,
                            final Path destination) {
        try {
            if (Files.exists(destination)) {
                Files.delete(destination);
            }
        } catch (final IOException e) {
            LOG.error("could not delete temp snapshotfile {}: {}", destination.toString(), e.getMessage());
            return false;
        }
        try {
            final GetObjectRequest request = GetObjectRequest.builder().bucket(bucketName).key(fileName).build();
            final GetObjectResponse getObjectResponse = s3Client.getObject(request, destination);
            LOG.debug("download {} from bucket {}: {}", fileName, bucketName, getObjectResponse.toString());
            return true;
        } catch (final RuntimeException e) {
            LOG.error("Failed to download {} from bucket {}: {}", fileName, bucketName, e.getMessage());
            return false;
        }
    }

    public void deleteAllObjectsInBucket(final String bucketName) {
        try {
            LOG.debug("deleting all objects in bucket {}", bucketName);
            deleteAllObjectsWithPrefixInBucket(bucketName, "");
            LOG.debug("files in bucket: {}", listAllFiles(bucketName));
        } catch (final RuntimeException e) {
            LOG.error("Error while deleting files from bucket: " + e.getMessage(), e);
        }
    }

    public void deleteAllObjectsWithPrefixInBucket(final String bucketName,
                                                   final String prefix) {

        ListObjectsV2Request request = ListObjectsV2Request.builder().bucket(bucketName).build();
        ListObjectsV2Response response;

        do {
            response = s3Client.listObjectsV2(request);
            if (response.hasContents()) {
                DeleteObjectsRequest deleteObjectsRequest = DeleteObjectsRequest.builder()
                        .bucket(bucketName)
                        .delete(builder()
                                .objects(convertS3ObjectsToObjectIdentifiers(response, prefix))
                                .build())
                        .build();
                final DeleteObjectsResponse deleteObjectsResponse = s3Client.deleteObjects(deleteObjectsRequest);
                LOG.debug("Deleted {} objects in bucket {} with prefix {}: {}", deleteObjectsResponse.deleted().size(), bucketName, prefix, deleteObjectsResponse);
            } else {
                LOG.debug("deleteAllObjectsWithPrefixInBucket listObjects found no keys in bucket {} with prefix {}: {}", bucketName, prefix, response);
            }
            String token = response.nextContinuationToken();
            request = request.toBuilder().continuationToken(token).build();
        } while (response.isTruncated());

    }

    public List<String> listAllFiles(final String bucketName) {
        return listAll(bucketName)
                .stream()
                .map(S3Object::key)
                .collect(toList());
    }


    public List<S3Object> listAll(final String bucketName) {
        ListObjectsV2Request request = ListObjectsV2Request.builder().bucket(bucketName).build();
        ListObjectsV2Response response;

        ArrayList<S3Object> s3Objects = new ArrayList<>();
        do {
            response = s3Client.listObjectsV2(request);
            if (response.hasContents()) {
                s3Objects.addAll(response.contents());
            }
            String token = response.nextContinuationToken();
            request = request.toBuilder().continuationToken(token).build();
        } while (response.isTruncated());

        return Collections.unmodifiableList(s3Objects);
    }



    private List<ObjectIdentifier> convertS3ObjectsToObjectIdentifiers(final ListObjectsV2Response listObjectsV2Response,
                                                                       final String prefix) {
        return listObjectsV2Response.contents()
                .stream()
                .filter(o -> o.key() != null && o.key().startsWith(prefix))
                .map(o -> ObjectIdentifier.builder().key(o.key()).build()).collect(toList());
    }
}
