package de.otto.synapse.example.producer;

import de.otto.synapse.message.Message;
import de.otto.synapse.sender.MessageSender;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;


public class ExampleProducerTest {

    ExampleProducer testee;
    MessageSender sender;

    @Before
    public void setUp() throws Exception {
        sender = mock(MessageSender.class);
        testee = new ExampleProducer(sender);
    }

    @Test
    public void shouldProduceEvent() throws Exception {
        // given

        // when
        testee.produceSampleData();

        //then
        verify(sender).send(any(Message.class));
    }


}
