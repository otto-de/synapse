package de.otto.edison.eventsourcing.aws.consumer;

import de.otto.edison.eventsourcing.consumer.MethodInvokingMessageConsumer;
import de.otto.edison.eventsourcing.message.Message;
import org.junit.Before;
import org.junit.Test;
import org.springframework.aop.support.AopUtils;

import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class MethodInvokingMessageConsumerTest {

    private boolean eventReceived;

    public void validMethod(final Message<String> message) {
        eventReceived = true;
    }

    public String validMethodWithReturnValue(final Message<String> message) {
        eventReceived = true;
        return "";
    }

    public void methodWithTooManyParameters(final Message<String> message, final String foo) {
        eventReceived = true;
    }

    public void methodWithMissingEventParam(final String event) {
        eventReceived = true;
    }

    @Before
    public void setup() {
        eventReceived = false;
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldBuildEventConsumerForValidMethod() throws NoSuchMethodException {
        final Method method = MethodInvokingMessageConsumerTest.class.getMethod("validMethod", Message.class);
        final Method method1 = AopUtils.selectInvocableMethod(method, MethodInvokingMessageConsumerTest.class);
        final MethodInvokingMessageConsumer eventConsumer = new MethodInvokingMessageConsumer(".*", String.class, this, method1);
        eventConsumer.accept(mock(Message.class));
        assertThat(eventReceived).isTrue();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldBuildEventConsumerAndIgnoreReturnValue() throws NoSuchMethodException {
        final Method method = MethodInvokingMessageConsumerTest.class.getMethod("validMethodWithReturnValue", Message.class);
        final MethodInvokingMessageConsumer eventConsumer = new MethodInvokingMessageConsumer(".*", String.class, this, method);
        eventConsumer.accept(mock(Message.class));
        assertThat(eventReceived).isTrue();
    }

    @Test(expected = IllegalArgumentException.class)
    @SuppressWarnings("unchecked")
    public void shouldFailBuildingEventConsumerWithTooManyArgs() throws NoSuchMethodException {
        final Method method = MethodInvokingMessageConsumerTest.class.getMethod("methodWithTooManyParameters", Message.class, String.class);
        new MethodInvokingMessageConsumer(".*", String.class, this, method);
    }

    @Test(expected = IllegalArgumentException.class)
    @SuppressWarnings("unchecked")
    public void shouldFailBuildingEventConsumerWithMissingEventArgs() throws NoSuchMethodException {
        final Method method = MethodInvokingMessageConsumerTest.class.getMethod("methodWithMissingEventParam", String.class);
        new MethodInvokingMessageConsumer(".*", String.class, this, method);
    }

    @Test(expected = NullPointerException.class)
    @SuppressWarnings("unchecked")
    public void shouldFailBuildingEventConsumerWithMissingMethod() throws NoSuchMethodException {
        new MethodInvokingMessageConsumer(".*", String.class, this,null);
    }

}

