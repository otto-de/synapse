package de.otto.synapse.example.producer;

import de.otto.synapse.endpoint.sender.AbstractMessageSenderEndpoint;
import de.otto.synapse.endpoint.sender.MessageSenderEndpoint;
import de.otto.synapse.message.Message;
import de.otto.synapse.translator.MessageTranslator;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nonnull;

import java.util.concurrent.CompletableFuture;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;


public class ExampleProducerTest {

    private ExampleProducer testee;
    private Message<String> sentMessage = null;
    @Before
    public void setUp() {
        final MessageTranslator<String> translator = MessageTranslator.of((payload -> "received"));

        final MessageSenderEndpoint sender = new AbstractMessageSenderEndpoint("test", translator) {
            protected CompletableFuture<Void> doSend(@Nonnull Message<String> message) {
                sentMessage = message;
                return completedFuture(null);
            }
        };
        testee = new ExampleProducer(sender);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldProduceEvent() throws Exception {
        // given

        // when
        testee.produceSampleData();

        //then
        assertThat(sentMessage.getKey(), is("1"));
        assertThat(sentMessage.getPayload(), is("received"));
    }

}
