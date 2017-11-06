package de.otto.edison.eventsourcing.consumer;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class MethodInvokingEventConsumer<T> implements EventConsumer<T> {

    private final String streamName;
    private final Object instance;
    private final Method method;

    public MethodInvokingEventConsumer(final String streamName, final Object instance, final Method method) {
        if (instance == null) {
            throw new NullPointerException("Unable to build MethodInvokingEventConsumer: instance parameter is null");
        }
        if (method == null) {
            throw new NullPointerException("Unable to build MethodInvokingEventConsumer: method parameter is null");
        }
        if (method.getParameterCount() != 1) {
            throw new IllegalArgumentException("Unable to build MethodInvokingEventConsumer: illegal number of arguments ");
        }
        final Class<?> paramType = method.getParameterTypes()[0];
        if (!paramType.equals(Event.class)) {
            throw new IllegalArgumentException("Unable to build MethodInvokingEventConsumer: expected parameter type is Event, not " + paramType.getName());
        }
        this.streamName = streamName;
        this.method = method;
        this.instance = instance;
    }

    /**
     * Returns the name of the EventSource.
     * <p>
     * For streaming event-sources, this is the name of the event stream.
     * </p>
     *
     * @return name
     */
    @Override
    public String streamName() {
        return streamName;
    }

    @Override
    public void accept(final Event<T> event) {
        try {
            method.invoke(instance, event);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new IllegalStateException(e);
        }
    }

}
