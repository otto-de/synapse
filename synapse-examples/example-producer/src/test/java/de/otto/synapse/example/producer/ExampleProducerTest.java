package de.otto.synapse.example.producer;

import de.otto.synapse.endpoint.MessageSenderEndpoint;
import de.otto.synapse.message.Message;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;


public class ExampleProducerTest {

    private ExampleProducer testee;
    private MessageSenderEndpoint sender;

    @Before
    public void setUp() throws Exception {
        sender = mock(MessageSenderEndpoint.class);
        testee = new ExampleProducer(sender);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldProduceEvent() throws Exception {
        // given

        // when
        testee.produceSampleData();

        //then
        verify(sender).send(any(Message.class));
    }


}
