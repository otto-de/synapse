package de.otto.edison.eventsourcing.aws.consumer;

import de.otto.edison.eventsourcing.consumer.MethodInvokingEventConsumer;
import de.otto.edison.eventsourcing.event.Event;
import org.junit.Before;
import org.junit.Test;
import org.springframework.aop.support.AopUtils;

import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class MethodInvokingEventConsumerTest {

    private boolean eventReceived;

    public void validMethod(final Event<String> event) {
        eventReceived = true;
    }

    public String validMethodWithReturnValue(final Event<String> event) {
        eventReceived = true;
        return "";
    }

    public void methodWithTooManyParameters(final Event<String> event, final String foo) {
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
        final Method method = MethodInvokingEventConsumerTest.class.getMethod("validMethod", Event.class);
        final Method method1 = AopUtils.selectInvocableMethod(method, MethodInvokingEventConsumerTest.class);
        final MethodInvokingEventConsumer eventConsumer = new MethodInvokingEventConsumer(".*", String.class, this, method1);
        eventConsumer.accept(mock(Event.class));
        assertThat(eventReceived).isTrue();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldBuildEventConsumerAndIgnoreReturnValue() throws NoSuchMethodException {
        final Method method = MethodInvokingEventConsumerTest.class.getMethod("validMethodWithReturnValue", Event.class);
        final MethodInvokingEventConsumer eventConsumer = new MethodInvokingEventConsumer(".*", String.class, this, method);
        eventConsumer.accept(mock(Event.class));
        assertThat(eventReceived).isTrue();
    }

    @Test(expected = IllegalArgumentException.class)
    @SuppressWarnings("unchecked")
    public void shouldFailBuildingEventConsumerWithTooManyArgs() throws NoSuchMethodException {
        final Method method = MethodInvokingEventConsumerTest.class.getMethod("methodWithTooManyParameters", Event.class, String.class);
        new MethodInvokingEventConsumer(".*", String.class, this, method);
    }

    @Test(expected = IllegalArgumentException.class)
    @SuppressWarnings("unchecked")
    public void shouldFailBuildingEventConsumerWithMissingEventArgs() throws NoSuchMethodException {
        final Method method = MethodInvokingEventConsumerTest.class.getMethod("methodWithMissingEventParam", String.class);
        new MethodInvokingEventConsumer(".*", String.class, this, method);
    }

    @Test(expected = NullPointerException.class)
    @SuppressWarnings("unchecked")
    public void shouldFailBuildingEventConsumerWithMissingMethod() throws NoSuchMethodException {
        new MethodInvokingEventConsumer(".*", String.class, this,null);
    }

}

