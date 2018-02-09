package de.otto.edison.eventsourcing.consumer;

import de.otto.edison.eventsourcing.event.Message;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Objects;
import java.util.regex.Pattern;

public class MethodInvokingEventConsumer<T> implements EventConsumer<T> {

    private final Pattern keyPattern;
    private final Class<T> payloadType;
    private final Object instance;
    private final Method method;

    public MethodInvokingEventConsumer(final String keyPattern,
                                       final Class<T> payloadType,
                                       final Object instance,
                                       final Method method) {
        Objects.requireNonNull(keyPattern, "keyPattern must not be null");
        Objects.requireNonNull(payloadType, "payloadType must not be null");
        Objects.requireNonNull(instance, "Unable to build MethodInvokingEventConsumer: instance parameter is null");
        Objects.requireNonNull(method, "Unable to build MethodInvokingEventConsumer: method parameter is null");

        if (method.getParameterCount() != 1) {
            throw new IllegalArgumentException("Unable to build MethodInvokingEventConsumer: illegal number of arguments ");
        }
        final Class<?> paramType = method.getParameterTypes()[0];
        if (!paramType.equals(Message.class)) {
            throw new IllegalArgumentException("Unable to build MethodInvokingEventConsumer: expected parameter type is Event, not " + paramType.getName());
        }

        this.keyPattern = Pattern.compile(keyPattern);
        this.payloadType = payloadType;
        this.method = method;
        this.instance = instance;
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
     * Returns the pattern of {@link Message#getKey()} () event keys} accepted by this consumer.
     *
     * @return Pattern
     */
    @Override
    public Pattern keyPattern() {
        return keyPattern;
    }

    @Override
    public void accept(final Message<T> message) {
        try {
            method.invoke(instance, message);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new IllegalStateException(e);
        }
    }

}
