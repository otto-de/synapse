package de.otto.synapse.endpoint;

import com.google.common.collect.ImmutableMap;
import de.otto.synapse.message.Header;
import de.otto.synapse.message.Message;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.stream.Stream;

import static de.otto.synapse.endpoint.MethodInvokingMessageInterceptorTest.TestInterceptors.*;
import static de.otto.synapse.message.Message.message;
import static org.junit.Assert.*;

public class MethodInvokingMessageInterceptorTest {

    static class TestInterceptors {

        public void voidInterceptor(final Message<String> message) {
        }

        public Message<String> interceptor(final Message<String> message) {
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
        Message<String> expected = message("foo", Header.requestHeader(ImmutableMap.of("h", "val")), "some payload");
        Message<String> intercepted = interceptor.intercept(expected);
        assertEquals(intercepted, expected);
    }

    @Test
    public void shouldInterceptMessagesWithMissingReturnTypeParam() {
        MethodInvokingMessageInterceptor interceptor = new MethodInvokingMessageInterceptor(new TestInterceptors(), method("interceptorWithoutReturnTypeParam"));
        Message<String> expected = message("foo", Header.requestHeader(ImmutableMap.of("h", "val")), "some payload");
        Message<String> intercepted = interceptor.intercept(expected);
        assertEquals(intercepted, expected);
    }

    @Test
    public void shouldInterceptMessagesWithMissingParamType() {
        MethodInvokingMessageInterceptor interceptor = new MethodInvokingMessageInterceptor(new TestInterceptors(), method("interceptorWithoutParamType"));
        Message<String> expected = message("foo", Header.requestHeader(ImmutableMap.of("h", "val")), "some payload");
        Message<String> intercepted = interceptor.intercept(expected);
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
        Message<String> message = message("foo", Header.requestHeader(ImmutableMap.of("h", "val")), "some payload");
        Message<String> intercepted = interceptor.intercept(message);
        assertNull(intercepted);
    }

    @Test
    public void shouldInterceptMessagesWithVoidInterceptor() {
        MethodInvokingMessageInterceptor interceptor = new MethodInvokingMessageInterceptor(new TestInterceptors(), method("voidInterceptor"));
        Message<String> expected = message("foo", Header.requestHeader(ImmutableMap.of("h", "val")), "some payload");
        Message<String> intercepted = interceptor.intercept(expected);
        assertEquals(intercepted, expected);
    }

}