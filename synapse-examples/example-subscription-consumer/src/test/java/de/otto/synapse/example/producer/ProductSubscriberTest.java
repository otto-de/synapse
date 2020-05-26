package de.otto.synapse.example.producer;

import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.sender.AbstractMessageSenderEndpoint;
import de.otto.synapse.endpoint.sender.MessageSenderEndpoint;
import de.otto.synapse.message.Message;
import de.otto.synapse.message.TextMessage;
import de.otto.synapse.state.ConcurrentMapStateRepository;
import de.otto.synapse.translator.MessageTranslator;
import org.junit.Before;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;

import static java.util.concurrent.CompletableFuture.completedFuture;


public class ProductSubscriberTest {

    private ProductSubscriber testee;
    private Message<String> sentMessage = null;
    @Before
    public void setUp() {
        final MessageTranslator<TextMessage> translator = (message -> TextMessage.of(message.getKey(), message.getHeader(), "received"));

        final MessageSenderEndpoint sender = new AbstractMessageSenderEndpoint("test", new MessageInterceptorRegistry(), translator) {
            protected CompletableFuture<Void> doSend(@Nonnull TextMessage message) {
                sentMessage = message;
                return completedFuture(null);
            }
        };

        testee = new ProductSubscriber(new ConcurrentMapStateRepository<>("test"), sender, "test-products", "test-subscribe");
    }


}
