package de.otto.synapse.consumer;

import de.otto.synapse.message.Message;
import jakarta.annotation.Nonnull;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import static java.util.Collections.synchronizedList;

public class TestMessageConsumer<T> implements MessageConsumer<T> {

    private final Class<T> payloadType;
    private final Pattern keyPattern;

    private List<Message<T>> consumedMessages = synchronizedList(new ArrayList<>());

    public static <T> TestMessageConsumer<T> testEventConsumer(final Class<T> payloadType) {
        return testEventConsumer(".*", payloadType);
    }

    public static <T> TestMessageConsumer<T> testEventConsumer(final String keyPattern,
                                                               final Class<T> payloadType) {
        return new TestMessageConsumer<>(keyPattern, payloadType);
    }

    public TestMessageConsumer(final String keyPattern, final Class<T> payloadType) {
        this.keyPattern = Pattern.compile(keyPattern);
        this.payloadType = payloadType;
    }

    /**
     * Returns the expected payload type of {@link Message events} consumed by this EventConsumer.
     *
     * @return payload type
     */
    @Nonnull
    @Override
    public Class<T> payloadType() {
        return payloadType;
    }

    /**
     * Returns the pattern of {@link Message#key() event keys} accepted by this consumer.
     *
     * @return Pattern
     */
    @Nonnull
    @Override
    public Pattern keyPattern() {
        return keyPattern;
    }

    public List<Message<T>> getConsumedMessages() {
        return consumedMessages;
    }

    @Override
    public void accept(Message<T> message) {
        consumedMessages.add(message);
    }

}
