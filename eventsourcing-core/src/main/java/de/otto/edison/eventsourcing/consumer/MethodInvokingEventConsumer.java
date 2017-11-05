package de.otto.edison.eventsourcing.consumer;

import org.slf4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static org.slf4j.LoggerFactory.getLogger;

public class MethodInvokingEventConsumer<T> implements EventConsumer<T> {

    private static final Logger LOG = getLogger(MethodInvokingEventConsumer.class);

    private final Object instance;
    private final Method method;

    public MethodInvokingEventConsumer(final Object instance, final Method method) {
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
        this.method = method;
        this.instance = instance;
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
