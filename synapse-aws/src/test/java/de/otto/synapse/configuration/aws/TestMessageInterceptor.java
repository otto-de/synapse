package de.otto.synapse.configuration.aws;

import de.otto.synapse.endpoint.MessageInterceptor;
import de.otto.synapse.message.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.synchronizedList;

public class TestMessageInterceptor implements MessageInterceptor {

    private List<Message<String>> interceptedMessages = synchronizedList(new ArrayList<Message<String>>());

    @Nullable
    @Override
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

    public int size() {
        return interceptedMessages.size();
    }
}
