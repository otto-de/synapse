package de.otto.synapse.endpoint.sender.aws;

import com.google.common.collect.ImmutableMap;
import de.otto.synapse.logging.LogHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResultEntry;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static de.otto.synapse.logging.LogHelper.info;

public class RetryPutRecordsKinesisClient {

    private static final Logger LOG = LoggerFactory.getLogger(RetryPutRecordsKinesisClient.class);

    private static final int MAX_RETRY_COUNT = 3;

    private final KinesisClient kinesisClient;
    private final boolean waitBeforeRetry;

    public RetryPutRecordsKinesisClient(KinesisClient kinesisClient) {
        this(kinesisClient, true);
    }

    public RetryPutRecordsKinesisClient(KinesisClient kinesisClient, boolean waitBeforeRetry) {
        this.kinesisClient = kinesisClient;
        this.waitBeforeRetry = waitBeforeRetry;
    }

    public void putRecords(Supplier<PutRecordsRequest> putRecordsRequestSupplier) {
        int retryStep = 0;
        PutRecordsRequest putRecordsRequest = putRecordsRequestSupplier.get();
        while (retryStep++ < MAX_RETRY_COUNT) {
            final long t1 = System.currentTimeMillis();
            try {
                final PutRecordsResponse response = kinesisClient.putRecords(putRecordsRequest);
                final long t2 = System.currentTimeMillis();
                 if (response.failedRecordCount() == 0) {
                    info(LOG, ImmutableMap.of("runtime", (t2-t1)), "Write events to Kinesis", null);
                    return;
                } else {
                    LOG.warn("Failed to write events to Kinesis: {}", response.toString());
                    List<PutRecordsRequestEntry> retryRecords = findFailedRecords(putRecordsRequest, response);
                    putRecordsRequest = PutRecordsRequest.builder()
                            .records(retryRecords)
                            .streamName(putRecordsRequest.streamName())
                            .build();
                    if (waitBeforeRetry) {
                        waitDependingOnRetryStep(retryStep);
                    }
                }
                if (retryStep == MAX_RETRY_COUNT) {
                    LOG.error("Failed to write events to Kinesis: {}", response.toString());
                    throw new IllegalStateException(String.format("failed to send records after %s retries", MAX_RETRY_COUNT));
                }
            } catch (SdkServiceException | SdkClientException e) {
                LogHelper.warn(LOG, ImmutableMap.of("records", String.valueOf(putRecordsRequest.records()), "recordsSize", String.valueOf(getRecordsSize(putRecordsRequest.records()))), "Failed to write events to Kinesis: %s", new Object[]{e.getMessage()});
                if (retryStep == MAX_RETRY_COUNT) {
                    LOG.error("Failed to write events to Kinesis: {}", e);
                    throw new IllegalStateException(String.format("failed to send records after %s retries", MAX_RETRY_COUNT));
                }
                putRecordsRequest = putRecordsRequestSupplier.get();
                if (waitBeforeRetry) {
                    waitDependingOnRetryStep(retryStep);
                }
            }

        }
    }

    private long getRecordsSize(final List<PutRecordsRequestEntry> records) {
        return records.stream().map(e->e.data().position()).mapToInt(Number::intValue).sum();
    }

    private List<PutRecordsRequestEntry> findFailedRecords(PutRecordsRequest putRecordsRequest, PutRecordsResponse response) {
        List<PutRecordsRequestEntry> retryRecords = new ArrayList<>();
        for (int i = 0; i < response.records().size(); ++i) {
            PutRecordsResultEntry record = response.records().get(i);
            if ("ProvisionedThroughputExceededException".equals(record.errorCode())) {
                retryRecords.add(putRecordsRequest.records().get(i));
            }
        }
        return retryRecords;
    }

    private void waitDependingOnRetryStep(int retryStep) {
        try {
            Thread.sleep((long)Math.pow(2, retryStep) * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
