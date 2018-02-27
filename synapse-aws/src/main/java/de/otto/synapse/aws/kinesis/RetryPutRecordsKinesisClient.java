package de.otto.synapse.aws.kinesis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResultEntry;

import java.util.ArrayList;
import java.util.List;

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

    public void putRecords(PutRecordsRequest putRecordsRequest) {
        int retryStep = 0;
        while (retryStep++ < MAX_RETRY_COUNT) {
            PutRecordsResponse response = kinesisClient.putRecords(putRecordsRequest);
            if (response.failedRecordCount() == 0) {
                return;
            } else {
                LOG.info("Failed to write events to Kinesis: {}", response.toString());
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
        }
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
