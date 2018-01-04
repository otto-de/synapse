package de.otto.edison.eventsourcing.consumer;

import java.util.regex.Pattern;

class TestEventConsumer<T> implements EventConsumer<T> {

    private final String streamName;
    private final Class<T> payloadType;
    private final Pattern keyPattern;

    public static TestEventConsumer<String> testEventConsumer(final String streamName) {
        return testEventConsumer(streamName, ".*", String.class);
    }

    public static <T> TestEventConsumer<T> testEventConsumer(final String streamName,
                                                             final Class<T> payloadType) {
        return testEventConsumer(streamName, ".*", payloadType);
    }

    public static <T> TestEventConsumer<T> testEventConsumer(final String streamName,
                                                             final String keyPattern,
                                                             final Class<T> payloadType) {
        return new TestEventConsumer<>(streamName, keyPattern, payloadType);
    }

    public TestEventConsumer(final String streamName, final String keyPattern, final Class<T> payloadType) {
        this.streamName = streamName;
        this.keyPattern = Pattern.compile(keyPattern);
        this.payloadType = payloadType;
    }

    @Override
    public String streamName() {
        return streamName;
    }

    /**
     * Returns the expected payload type of {@link Event events} consumed by this EventConsumer.
     *
     * @return payload type
     */
    @Override
    public Class<T> payloadType() {
        return payloadType;
    }

    /**
     * Returns the pattern of {@link Event#key() event keys} accepted by this consumer.
     *
     * @return Pattern
     */
    @Override
    public Pattern keyPattern() {
        return keyPattern;
    }

    @Override
    public void accept(Event<T> myPayloadEvent) {
        // do nothing here for tests
    }

}