package de.otto.synapse.configuration.kinesis;

import de.otto.synapse.endpoint.MessageInterceptor;
import de.otto.synapse.message.TextMessage;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.synchronizedList;

public class TestMessageInterceptor implements MessageInterceptor {

    private List<TextMessage> interceptedMessages = synchronizedList(new ArrayList<>());

    @Nullable
    @Override
    public TextMessage intercept(@Nonnull TextMessage message) {
        interceptedMessages.add(message);
        return message;
    }

    public List<TextMessage> getInterceptedMessages() {
        return interceptedMessages;
    }

    public void clear() {
        interceptedMessages.clear();
    }

}
