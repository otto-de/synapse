package de.otto.synapse.endpoint;

import de.otto.synapse.message.Message;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class InterceptorChainTest {

    @Test
    @SuppressWarnings("unchecked")
    public void shouldBuildEmptyChain() {
        final InterceptorChain chain = new InterceptorChain();
        final Message message = mock(Message.class);
        final Message intercepted = chain.intercept(message);
        verifyZeroInteractions(message);
        assertThat(message, is(intercepted));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldReturnNull() {
        final MessageInterceptor interceptor = mock(MessageInterceptor.class);
        when(interceptor.intercept(any(Message.class))).thenReturn(null);
        final InterceptorChain chain = new InterceptorChain();
        chain.register(interceptor);
        assertThat(chain.intercept(someMessage("foo")), is(nullValue()));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldStopInterceptingOnNull() {
        final MessageInterceptor first = mock(MessageInterceptor.class);
        when(first.intercept(any(Message.class))).thenReturn(null);
        final MessageInterceptor second = mock(MessageInterceptor.class);
        final InterceptorChain chain = new InterceptorChain();
        chain.register(first);
        chain.register(second);
        assertThat(chain.intercept(someMessage("foo")), is(nullValue()));
        verifyZeroInteractions(second);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldReturnResultFromLastInterceptor() {
        final MessageInterceptor first = mock(MessageInterceptor.class);
        when(first.intercept(any(Message.class))).thenReturn(someMessage("foo"));
        final MessageInterceptor second = mock(MessageInterceptor.class);
        when(second.intercept(any(Message.class))).thenReturn(someMessage("bar"));
        final InterceptorChain chain = new InterceptorChain();
        chain.register(first);
        chain.register(second);
        //noinspection ConstantConditions
        assertThat(chain.intercept(someMessage("foo")).getKey(), is("bar"));
    }

    @SuppressWarnings("unchecked")
    private Message<String> someMessage(final String key) {
        return Message.message(key, null);
    }
}