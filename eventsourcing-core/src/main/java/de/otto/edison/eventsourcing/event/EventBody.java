package de.otto.edison.eventsourcing.event;

import java.util.Objects;

public class EventBody<T> {
    private final String key;
    private final T payload;

    public EventBody(String key, T payload) {
        this.key = key;
        this.payload = payload;
    }

    public static <T> EventBody<T> eventBody(String key, T payload) {
        return new EventBody<>(key, payload);
    }

    public String getKey() {
        return key;
    }

    public T getPayload() {
        return payload;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EventBody<?> eventBody = (EventBody<?>) o;
        return Objects.equals(key, eventBody.key) &&
                Objects.equals(payload, eventBody.payload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, payload);
    }

    @Override
    public String toString() {
        return "EventBody{" +
                "key='" + key + '\'' +
                ", payload=" + payload +
                '}';
    }
}
