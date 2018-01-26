package de.otto.edison.eventsourcing.example.producer;

import de.otto.edison.eventsourcing.consumer.EventConsumer;
import de.otto.edison.eventsourcing.consumer.EventSource;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = Server.class)
public class ExampleProducerIntegrationTest {

    @Autowired
    ExampleProducer producer;
    @MockBean
    EventConsumer consumer;
    @Autowired
    EventSource productEventSource;

    @Before
    public void setUp() throws Exception {

    }

    @Test
    public void shouldProduceEvent() throws Exception {
        // given
        producer.produceSampleData();

        // when

        //then

    }
}
