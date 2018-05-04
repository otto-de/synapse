package de.otto.synapse.client.aws;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResultEntry;

import java.util.ArrayList;
import java.util.List;

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

    public void putRecords(PutRecordsRequest putRecordsRequest) {
        int retryStep = 0;
        while (retryStep++ < MAX_RETRY_COUNT) {
            final long t1 = System.currentTimeMillis();
            PutRecordsResponse response = kinesisClient.putRecords(putRecordsRequest);
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
