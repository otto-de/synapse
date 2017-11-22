package de.otto.edison.eventsourcing.consumer;

import java.util.function.Consumer;

class TestEventConsumer<T> implements EventConsumer<T> {

    private static final String TEST_STREAM_NAME = "test-stream";

    private String streamName = TEST_STREAM_NAME;

    public TestEventConsumer setStreamName(String streamName) {
        this.streamName = streamName;
        return this;
    }

    @Override
    public String streamName() {
        return streamName;
    }

    @Override
    public Consumer<Event<T>> consumerFunction() {
        return this::accept;
    }

    public void accept(Event<T> myPayloadEvent) {
        // do nothing here for tests
    }

}