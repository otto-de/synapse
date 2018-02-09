package de.otto.edison.eventsourcing.consumer;

import de.otto.edison.eventsourcing.event.Message;

import java.util.regex.Pattern;

class TestEventConsumer<T> implements EventConsumer<T> {

    private final Class<T> payloadType;
    private final Pattern keyPattern;

    public static <T> TestEventConsumer<T> testEventConsumer(final Class<T> payloadType) {
        return testEventConsumer(".*", payloadType);
    }

    public static <T> TestEventConsumer<T> testEventConsumer(final String keyPattern,
                                                             final Class<T> payloadType) {
        return new TestEventConsumer<>(keyPattern, payloadType);
    }

    public TestEventConsumer(final String keyPattern, final Class<T> payloadType) {
        this.keyPattern = Pattern.compile(keyPattern);
        this.payloadType = payloadType;
    }

    /**
     * Returns the expected payload type of {@link Message events} consumed by this EventConsumer.
     *
     * @return payload type
     */
    @Override
    public Class<T> payloadType() {
        return payloadType;
    }

    /**
     * Returns the pattern of {@link Message#key() event keys} accepted by this consumer.
     *
     * @return Pattern
     */
    @Override
    public Pattern keyPattern() {
        return keyPattern;
    }

    @Override
    public void accept(Message<T> myPayloadMessage) {
        // do nothing here for tests
    }

}
