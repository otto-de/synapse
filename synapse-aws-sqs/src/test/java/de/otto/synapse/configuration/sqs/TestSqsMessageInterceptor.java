package de.otto.synapse.configuration.sqs;

import de.otto.synapse.annotation.MessageInterceptor;
import de.otto.synapse.message.Message;
import org.springframework.stereotype.Component;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.synchronizedList;

@Component
public class TestSqsMessageInterceptor {

    private List<Message<String>> interceptedMessages = synchronizedList(new ArrayList<>());

    @MessageInterceptor
    public Message<String> intercept(@Nonnull Message<String> message) {
        interceptedMessages.add(message);
        return message;
    }

    public List<Message<String>> getInterceptedMessages() {
        return interceptedMessages;
    }

    public void clear() {
        interceptedMessages.clear();
    }

}
