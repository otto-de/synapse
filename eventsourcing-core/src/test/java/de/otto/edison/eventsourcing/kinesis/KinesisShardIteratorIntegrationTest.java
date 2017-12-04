package de.otto.edison.eventsourcing.kinesis;


import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.KinesisException;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

@RunWith(SpringRunner.class)
@ActiveProfiles("test")
@EnableAutoConfiguration
@ComponentScan(basePackages = {"de.otto.edison.eventsourcing"})
@SpringBootTest(classes = {
        KinesisShardIteratorIntegrationTest.class,
        KinesisShardIteratorIntegrationTest.TestConfiguration.class
})
public class KinesisShardIteratorIntegrationTest {

    @Autowired
    KinesisClient kinesisClient;

    @Autowired
    private KinesisShardIterator kinesisShardIterator;


    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void shouldRetryReadingIteratorOnKinesisException() {
        // given
        GetRecordsResponse response = GetRecordsResponse.builder()
                .records()
                .nextShardIterator("nextIteratorId")
                .build();

        when(kinesisClient.getRecords(any())).thenThrow(new KinesisException("forced test exception")).thenReturn(response);

        // when
        kinesisShardIterator.next();

        // then
        verify(kinesisClient, times(2)).getRecords(any());
        assertThat(kinesisShardIterator.getId(), is("nextIteratorId"));

    }

    public static class TestConfiguration {

        @Bean
        public KinesisShardIterator kinesisShardIterator(KinesisClient kinesisClient) {
            return new KinesisShardIterator(kinesisClient, "1");
        }

        @Bean
        public KinesisClient kinesisClient() {
            return mock(KinesisClient.class);
        }

    }
}
