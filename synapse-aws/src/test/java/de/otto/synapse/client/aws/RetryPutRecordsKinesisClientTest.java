package de.otto.synapse.client.aws;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResultEntry;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class RetryPutRecordsKinesisClientTest {

    @Mock
    private KinesisClient kinesisClient;

    private RetryPutRecordsKinesisClient retryPutRecordsKinesisClient;

    @Before
    public void setUp() throws Exception {
        initMocks(this);
        retryPutRecordsKinesisClient = new RetryPutRecordsKinesisClient(kinesisClient, false);
    }

    @Test
    public void shouldNotRetryOnSuccess() throws Exception {
        // given
        when(kinesisClient.putRecords(any(PutRecordsRequest.class)))
                .thenReturn(PutRecordsResponse.builder().failedRecordCount(0).build());

        // when
        PutRecordsRequest putRecordsRequest = PutRecordsRequest.builder().build();
        retryPutRecordsKinesisClient.putRecords(putRecordsRequest);

        // then
        verify(kinesisClient).putRecords(putRecordsRequest);
    }

    @Test
    public void shouldRetryOnFailure() throws Exception {
        // given
        when(kinesisClient.putRecords(any(PutRecordsRequest.class)))
                .thenReturn(PutRecordsResponse.builder().failedRecordCount(1).records(emptyList()).build())
                .thenReturn(PutRecordsResponse.builder().failedRecordCount(0).records(emptyList()).build());

        // when
        PutRecordsRequest putRecordsRequest = PutRecordsRequest.builder().records(emptyList()).build();
        retryPutRecordsKinesisClient.putRecords(putRecordsRequest);

        // then
        verify(kinesisClient, times(2)).putRecords(putRecordsRequest);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldRetryThreeTimeMaxOnFailure() throws Exception {
        // given
        when(kinesisClient.putRecords(any(PutRecordsRequest.class)))
                .thenReturn(PutRecordsResponse.builder().failedRecordCount(1).records(emptyList()).build());

        // when
        PutRecordsRequest putRecordsRequest = PutRecordsRequest.builder().records(emptyList()).build();
        try {
            retryPutRecordsKinesisClient.putRecords(putRecordsRequest);
        } catch (Exception e) {
            // then
            verify(kinesisClient, times(3)).putRecords(putRecordsRequest);
            throw e;
        }
        fail();
    }

    @Test
    public void shouldOnlyResendFailedRecords() throws Exception {
        // given
        PutRecordsRequest putRecordsRequest = PutRecordsRequest.builder().streamName("test-stream").records(asList(
                        PutRecordsRequestEntry.builder().partitionKey("1").build(),
                        PutRecordsRequestEntry.builder().partitionKey("2").build(),
                        PutRecordsRequestEntry.builder().partitionKey("3").build(),
                        PutRecordsRequestEntry.builder().partitionKey("4").build()
                )).build();
        PutRecordsResponse putRecordsResponse = PutRecordsResponse.builder().failedRecordCount(4).records(asList(
                PutRecordsResultEntry.builder().errorCode(null).build(),
                PutRecordsResultEntry.builder().errorCode("ProvisionedThroughputExceededException").build(),
                PutRecordsResultEntry.builder().errorCode(null).build(),
                PutRecordsResultEntry.builder().errorCode("ProvisionedThroughputExceededException").build()
                )).build();
        when(kinesisClient.putRecords(putRecordsRequest)).thenReturn(putRecordsResponse);

        PutRecordsRequest expectedRetriedPutRecordsRequest = PutRecordsRequest.builder().streamName("test-stream").records(asList(
                PutRecordsRequestEntry.builder().partitionKey("2").build(),
                PutRecordsRequestEntry.builder().partitionKey("4").build()
        )).build();
        PutRecordsResponse putRecordsResponseForRetry = PutRecordsResponse.builder().failedRecordCount(0).records(asList(
                PutRecordsResultEntry.builder().errorCode(null).build(),
                PutRecordsResultEntry.builder().errorCode(null).build()
        )).build();
        when(kinesisClient.putRecords(expectedRetriedPutRecordsRequest)).thenReturn(putRecordsResponseForRetry);

        // when
        retryPutRecordsKinesisClient.putRecords(putRecordsRequest);

        // then
        verify(kinesisClient).putRecords(putRecordsRequest);
        verify(kinesisClient).putRecords(expectedRetriedPutRecordsRequest);
        verifyNoMoreInteractions(kinesisClient);
    }

    @Test
    public void shouldRetryOnKinesisException() throws Exception {
        // given
        when(kinesisClient.putRecords(any(PutRecordsRequest.class)))
                .thenThrow(new SdkClientException("Unable to execute HTTP request: The target server failed to respond"))
                .thenReturn(PutRecordsResponse.builder().failedRecordCount(0).records(emptyList()).build());

        // when
        PutRecordsRequest putRecordsRequest = PutRecordsRequest.builder().records(emptyList()).build();
        retryPutRecordsKinesisClient.putRecords(putRecordsRequest);

        // then
        verify(kinesisClient, times(2)).putRecords(putRecordsRequest);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldRetryThreeTimeMaxOnKinesisException() throws Exception {
        // given
        when(kinesisClient.putRecords(any(PutRecordsRequest.class)))
                .thenThrow(new SdkClientException("Unable to execute HTTP request: The target server failed to respond"));

        // when
        PutRecordsRequest putRecordsRequest = PutRecordsRequest.builder().records(emptyList()).build();
        try {
            retryPutRecordsKinesisClient.putRecords(putRecordsRequest);
        } catch (Exception e) {
            // then
            verify(kinesisClient, times(3)).putRecords(putRecordsRequest);
            throw e;
        }
        fail();
    }
}