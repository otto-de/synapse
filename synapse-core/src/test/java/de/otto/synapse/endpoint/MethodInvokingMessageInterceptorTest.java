package de.otto.synapse.endpoint;

import com.google.common.collect.ImmutableMap;
import de.otto.synapse.message.Header;
import de.otto.synapse.message.Message;
import de.otto.synapse.message.TextMessage;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.stream.Stream;

import static de.otto.synapse.endpoint.MethodInvokingMessageInterceptorTest.TestInterceptors.method;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class MethodInvokingMessageInterceptorTest {

    static class TestInterceptors {

        public void voidInterceptor(final Message<String> message) {
        }

        public Message<String> interceptor(final Message<String> message) {
            return message;
        }

        public TextMessage textMessageInterceptor(final TextMessage message) {
            return message;
        }

        public Message interceptorWithoutReturnTypeParam(final Message<String> message) {
            return message;
        }

        public Message<String> interceptorWithoutParamType(final Message message) {
            return message;
        }

        public Message<String> illegalGenericParamInterceptor(final Message<Integer> message) {
            return null;
        }

        public Message<String> illegalParamInterceptor(final String message) {
            return null;
        }

        public Message<String> illegalMultipleParamsInterceptor(final Message<String> message, final String text) {
            return null;
        }

        public Message<Integer> illegalReturnTypeInterceptor(final Message<String> message) {
            return null;
        }

        public Message<String> filter(final Message<String> message) {
            return null;
        }

        static Method method(final String methodName) {
            return Stream.of(TestInterceptors.class.getMethods()).filter(m -> methodName.equals(m.getName())).findFirst().get();
        }
    }

    @Test
    public void shouldInterceptMessages() {
        MethodInvokingMessageInterceptor interceptor = new MethodInvokingMessageInterceptor(new TestInterceptors(), method("interceptor"));
        TextMessage expected = TextMessage.of("foo", Header.of(ImmutableMap.of("h", "val")), "some payload");
        TextMessage intercepted = interceptor.intercept(expected);
        assertEquals(intercepted, expected);
    }

    @Test
    public void shouldInterceptTextMessages() {
        MethodInvokingMessageInterceptor interceptor = new MethodInvokingMessageInterceptor(new TestInterceptors(), method("textMessageInterceptor"));
        TextMessage expected = TextMessage.of("foo", Header.of(ImmutableMap.of("h", "val")), "some payload");
        TextMessage intercepted = interceptor.intercept(expected);
        assertEquals(intercepted, expected);
    }

    @Test
    public void shouldInterceptMessagesWithMissingReturnTypeParam() {
        MethodInvokingMessageInterceptor interceptor = new MethodInvokingMessageInterceptor(new TestInterceptors(), method("interceptorWithoutReturnTypeParam"));
        TextMessage expected = TextMessage.of("foo", Header.of(ImmutableMap.of("h", "val")), "some payload");
        TextMessage intercepted = interceptor.intercept(expected);
        assertEquals(intercepted, expected);
    }

    @Test
    public void shouldInterceptMessagesWithMissingParamType() {
        MethodInvokingMessageInterceptor interceptor = new MethodInvokingMessageInterceptor(new TestInterceptors(), method("interceptorWithoutParamType"));
        TextMessage expected = TextMessage.of("foo", Header.of(ImmutableMap.of("h", "val")), "some payload");
        TextMessage intercepted = interceptor.intercept(expected);
        assertEquals(intercepted, expected);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailWithIllegalReturnTypeInterceptor() {
        new MethodInvokingMessageInterceptor(new TestInterceptors(), method("illegalReturnTypeInterceptor"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailWithIllegalGenericParamInterceptor() {
        new MethodInvokingMessageInterceptor(new TestInterceptors(), method("illegalGenericParamInterceptor"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailWithIllegalParamInterceptor() {
        new MethodInvokingMessageInterceptor(new TestInterceptors(), method("illegalParamInterceptor"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailWithIllegalMultipleParamsInterceptor() {
        new MethodInvokingMessageInterceptor(new TestInterceptors(), method("illegalMultipleParamsInterceptor"));
    }

    @Test
    public void shouldFilterMessages() {
        MethodInvokingMessageInterceptor interceptor = new MethodInvokingMessageInterceptor(new TestInterceptors(), method("filter"));
        TextMessage message = TextMessage.of("foo", Header.of(ImmutableMap.of("h", "val")), "some payload");
        TextMessage intercepted = interceptor.intercept(message);
        assertNull(intercepted);
    }

    @Test
    public void shouldInterceptMessagesWithVoidInterceptor() {
        MethodInvokingMessageInterceptor interceptor = new MethodInvokingMessageInterceptor(new TestInterceptors(), method("voidInterceptor"));
        TextMessage expected = TextMessage.of("foo", Header.of(ImmutableMap.of("h", "val")), "some payload");
        TextMessage intercepted = interceptor.intercept(expected);
        assertEquals(intercepted, expected);
    }

}