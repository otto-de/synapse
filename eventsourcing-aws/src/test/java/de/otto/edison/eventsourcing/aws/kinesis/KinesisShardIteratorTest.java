package de.otto.edison.eventsourcing.aws.kinesis;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.KinesisException;
import software.amazon.awssdk.services.kinesis.model.Record;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class KinesisShardIteratorTest {

    @Mock
    private KinesisClient kinesisClient;

    private KinesisShardIterator kinesisShardIterator;

    @Before
    public void setUp() {
        kinesisShardIterator = new KinesisShardIterator(kinesisClient, "someId");
    }

    @Test
    public void shouldFetchRecords() {
        // given
        GetRecordsResponse response = GetRecordsResponse.builder()
                .records(Record.builder()
                        .sequenceNumber("someSeqNumber")
                        .build())
                .build();

        when(kinesisClient.getRecords(any(GetRecordsRequest.class))).thenReturn(response);

        // when
        GetRecordsResponse fetchedResponse = kinesisShardIterator.next();

        // then
        assertThat(fetchedResponse, is(response));
        GetRecordsRequest expectedRequest = GetRecordsRequest.builder()
                .shardIterator("someId")
                .limit(KinesisShardIterator.FETCH_RECORDS_LIMIT)
                .build();
        verify(kinesisClient).getRecords(expectedRequest);
    }

    @Test
    public void shouldIterateToNextId() {
        // given
        GetRecordsResponse response = GetRecordsResponse.builder()
                .records()
                .nextShardIterator("nextIteratorId")
                .build();

        when(kinesisClient.getRecords(any(GetRecordsRequest.class))).thenReturn(response);

        // when
        kinesisShardIterator.next();

        // then
        assertThat(kinesisShardIterator.getId(), is("nextIteratorId"));
    }


    @Test
    public void shouldRetryReadingIteratorOnKinesisException() {
        // given
        GetRecordsResponse response = GetRecordsResponse.builder()
                .records()
                .nextShardIterator("nextIteratorId")
                .build();

        when(kinesisClient.getRecords(any(GetRecordsRequest.class)))
                .thenThrow(new KinesisException("forced test exception"))
                .thenThrow(new KinesisException("forced test exception"))
                .thenThrow(new KinesisException("forced test exception"))
                .thenThrow(new KinesisException("forced test exception"))
                .thenReturn(response);

        // when
        kinesisShardIterator.next();

        // then
        verify(kinesisClient, times(5)).getRecords(any(GetRecordsRequest.class));
        assertThat(kinesisShardIterator.getId(), is("nextIteratorId"));

    }
}
