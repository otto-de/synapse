package de.otto.synapse.endpoint;

import de.otto.synapse.message.Key;
import org.junit.Test;

import static de.otto.synapse.endpoint.MessageFilter.messageFilter;
import static de.otto.synapse.endpoint.MessageInterceptorRegistration.*;
import static de.otto.synapse.message.Message.message;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class MessageInterceptorRegistrationTest {

    @Test
    public void shouldCreateMessageFilterRegistration() {
        final MessageFilter messageFilter = messageFilter((m) -> m.getKey().equals(Key.of("foo")));

        final MessageInterceptorRegistration registration = senderChannelsWith(messageFilter);
        assertThat(registration.getInterceptor().intercept(message(Key.of("foo"), null)), is(notNullValue()));
        assertThat(registration.getInterceptor().intercept(message(Key.of("bar"), null)), is(nullValue()));
    }

    @Test
    public void shouldCreateSenderRegistration() {
        final MessageInterceptor messageInterceptor = (m) -> m;
        final MessageInterceptorRegistration registration = senderChannelsWith(messageInterceptor);
        assertThat(registration.isEnabledFor("any channel", EndpointType.SENDER), is(true));
        assertThat(registration.isEnabledFor("any channel", EndpointType.RECEIVER), is(false));
        assertThat(registration.getInterceptor(), is(messageInterceptor));
    }

    @Test
    public void shouldCreateReceiverRegistration() {
        final MessageInterceptor messageInterceptor = (m) -> m;
        final MessageInterceptorRegistration registration = receiverChannelsWith(messageInterceptor);
        assertThat(registration.isEnabledFor("any channel", EndpointType.SENDER), is(false));
        assertThat(registration.isEnabledFor("any channel", EndpointType.RECEIVER), is(true));
        assertThat(registration.getInterceptor(), is(messageInterceptor));
    }

    @Test
    public void shouldCreateChannelRegistration() {
        final MessageInterceptor messageInterceptor = (m) -> m;
        final MessageInterceptorRegistration registration = allChannelsWith(messageInterceptor);
        assertThat(registration.isEnabledFor("any channel", EndpointType.SENDER), is(true));
        assertThat(registration.isEnabledFor("any channel", EndpointType.RECEIVER), is(true));
        assertThat(registration.getInterceptor(), is(messageInterceptor));
    }

    @Test
    public void shouldCreateMatchingSenderRegistration() {
        final MessageInterceptor messageInterceptor = (m) -> m;
        final MessageInterceptorRegistration registration = matchingSenderChannelsWith("any.*", messageInterceptor);
        assertThat(registration.isEnabledFor("any channel", EndpointType.SENDER), is(true));
        assertThat(registration.isEnabledFor("other channel", EndpointType.SENDER), is(false));
        assertThat(registration.isEnabledFor("any channel", EndpointType.RECEIVER), is(false));
        assertThat(registration.getInterceptor(), is(messageInterceptor));
    }

    @Test
    public void shouldCreateMatchingReceiverRegistration() {
        final MessageInterceptor messageInterceptor = (m) -> m;
        final MessageInterceptorRegistration registration = matchingReceiverChannelsWith("any.*", messageInterceptor);
        assertThat(registration.isEnabledFor("any channel", EndpointType.SENDER), is(false));
        assertThat(registration.isEnabledFor("any channel", EndpointType.RECEIVER), is(true));
        assertThat(registration.isEnabledFor("other channel", EndpointType.RECEIVER), is(false));
        assertThat(registration.getInterceptor(), is(messageInterceptor));
    }

    @Test
    public void shouldCreateMatchingChannelRegistration() {
        final MessageInterceptor messageInterceptor = (m) -> m;
        final MessageInterceptorRegistration registration = matchingChannelsWith("any.*", messageInterceptor);
        assertThat(registration.isEnabledFor("any channel", EndpointType.SENDER), is(true));
        assertThat(registration.isEnabledFor("other channel", EndpointType.SENDER), is(false));
        assertThat(registration.isEnabledFor("any channel", EndpointType.RECEIVER), is(true));
        assertThat(registration.isEnabledFor("other channel", EndpointType.RECEIVER), is(false));
        assertThat(registration.getInterceptor(), is(messageInterceptor));
    }

}