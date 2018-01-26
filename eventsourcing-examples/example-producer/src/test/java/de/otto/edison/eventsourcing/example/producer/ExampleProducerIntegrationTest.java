package de.otto.edison.eventsourcing.example.producer;

import de.otto.edison.eventsourcing.EventSender;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class ExampleProducerIntegrationTest {

    ExampleProducer testee;
    EventSender sender;

    @Before
    public void setUp() throws Exception {
        sender = mock(EventSender.class);
        testee = new ExampleProducer(sender);
    }

    @Test
    public void shouldProduceEvent() throws Exception {
        // given

        // when
        testee.produceSampleData();

        //then
        verify(sender).sendEvent(anyString(), any(Long.class));
    }
}
