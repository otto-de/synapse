package de.otto.synapse.endpoint;

import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.message.Message;
import de.otto.synapse.message.TextMessage;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Objects;

/**
 * A {@link MessageConsumer} that is calling a method of a class instance for every accepted {@link Message}
 * <p>
 *     <img src="http://www.enterpriseintegrationpatterns.com/img/MessageEndpointSolution.gif" alt="MesageConsumer">
 * </p>
 */
public class MethodInvokingMessageInterceptor implements MessageInterceptor {

    private final Object instance;
    private final Method method;
    private final boolean returnsMessage;

    public MethodInvokingMessageInterceptor(final Object instance,
                                            final Method method) {
        Objects.requireNonNull(instance, "Unable to build MethodInvokingMessageInterceptor: instance parameter is null");
        Objects.requireNonNull(method, "Unable to build MethodInvokingMessageInterceptor: method parameter is null");

        if (method.getReturnType() != void.class && !Message.class.isAssignableFrom(method.getReturnType())) {
            throw new IllegalArgumentException("Unable to build MethodInvokingMessageInterceptor: return type of the annotated method must be void or Message<String> or TextMessage");
        }
        if (method.getParameterCount() != 1) {
            throw new IllegalArgumentException("Unable to build MethodInvokingMessageInterceptor: illegal number of arguments, expected exactly one parameter with type Message<String> or TextMessage");
        }

        assertIsMessageWithStringTypeParam(method.getGenericReturnType());
        assertIsMessageOrVoid(method.getReturnType());

        assertIsMessageWithStringTypeParam(method.getGenericParameterTypes()[0]);
        assertIsMessage(method.getParameterTypes()[0]);

        this.method = method;
        this.instance = instance;
        this.returnsMessage = method.getReturnType() != Void.class && method.getReturnType() != void.class;
    }

    private void assertIsMessageWithStringTypeParam(final Type type) {
        if (type instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) type;
            boolean isMessageType = !parameterizedType.getRawType().equals(Message.class);
            boolean hasStringArgument = !Arrays.equals(parameterizedType.getActualTypeArguments(), new Type[]{String.class});
            if (isMessageType || hasStringArgument) {
                throw new IllegalArgumentException("Unable to build MethodInvokingMessageInterceptor: parameter " + type);
            }
        }
    }

    private void assertIsMessage(Class<?> paramType) {
        if (!Message.class.isAssignableFrom(paramType)) {
            throw new IllegalArgumentException("Unable to build MethodInvokingMessageInterceptor: expected parameter type is Message, not " + paramType.getName());
        }
    }

    private void assertIsMessageOrVoid(Class<?> paramType) {
        if (!Message.class.isAssignableFrom(paramType) && !paramType.equals(void.class) && !paramType.equals(Void.class)) {
            throw new IllegalArgumentException("Unable to build MethodInvokingMessageInterceptor: expected parameter type is Message, not " + paramType.getName());
        }
    }

    @Nullable
    @Override
    @SuppressWarnings("unchecked")
    public TextMessage intercept(@Nonnull TextMessage message) {
        try {
            if (returnsMessage) {
                final Message<String> interceptedMessage = (Message<String>) method.invoke(instance, message);
                if (interceptedMessage == null || interceptedMessage == message || interceptedMessage instanceof TextMessage) {
                    return (TextMessage) interceptedMessage;
                } else {
                    return TextMessage.of(interceptedMessage);
                }
            } else {
                method.invoke(instance, message);
                return message;
            }
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new IllegalStateException(e);
        }
    }
}
