package de.otto.edison.eventsourcing.kinesis;


import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.KinesisException;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class KinesisShardIteratorIntegrationTest {

    @Mock
    private KinesisClient kinesisClient;

    private KinesisShardIterator kinesisShardIterator;


    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        kinesisShardIterator = new KinesisShardIterator(kinesisClient, "1");
    }

    @Test
    public void shouldRetryReadingIteratorOnKinesisException() {
        // given
        GetRecordsResponse response = GetRecordsResponse.builder()
                .records()
                .nextShardIterator("nextIteratorId")
                .build();

        when(kinesisClient.getRecords(any()))
                .thenThrow(new KinesisException("forced test exception"))
                .thenThrow(new KinesisException("forced test exception"))
                .thenThrow(new KinesisException("forced test exception"))
                .thenThrow(new KinesisException("forced test exception"))
                .thenReturn(response);

        // when
        kinesisShardIterator.next();

        // then
        verify(kinesisClient, times(5)).getRecords(any());
        assertThat(kinesisShardIterator.getId(), is("nextIteratorId"));

    }

}
