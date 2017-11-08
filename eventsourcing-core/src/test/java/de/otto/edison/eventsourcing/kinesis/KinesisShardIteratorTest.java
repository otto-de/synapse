package de.otto.edison.eventsourcing.kinesis;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.Record;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class KinesisShardIteratorTest {

    @Mock
    private KinesisClient kinesisClient;

    private KinesisShardIterator kinesisShardIterator;

    @Before
    public void setUp() throws Exception {
        kinesisShardIterator = new KinesisShardIterator(kinesisClient, "someId");
    }

    @Test
    public void shouldFetchRecords() throws Exception {
        // given
        GetRecordsResponse response = GetRecordsResponse.builder()
                .records(Record.builder()
                        .sequenceNumber("someSeqNumber")
                        .build())
                .build();

        when(kinesisClient.getRecords(any())).thenReturn(response);

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
    public void shouldIterateToNextId() throws Exception {
        // given
        GetRecordsResponse response = GetRecordsResponse.builder()
                .records()
                .nextShardIterator("nextIteratorId")
                .build();

        when(kinesisClient.getRecords(any())).thenReturn(response);

        // when
        kinesisShardIterator.next();

        // then
        assertThat(kinesisShardIterator.getId(), is("nextIteratorId"));
    }
}