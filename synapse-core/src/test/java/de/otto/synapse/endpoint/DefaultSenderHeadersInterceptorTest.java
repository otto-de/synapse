package de.otto.synapse.endpoint;

import de.otto.synapse.configuration.SynapseProperties;
import de.otto.synapse.endpoint.DefaultSenderHeadersInterceptor.Capability;
import de.otto.synapse.message.Message;
import de.otto.synapse.testsupport.TestClock;
import org.junit.Test;

import java.time.Clock;
import java.util.EnumSet;

import static de.otto.synapse.message.DefaultHeaderAttr.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class DefaultSenderHeadersInterceptorTest {

    @Test
    public void shouldAddDefaultHeaders() {
        // given
        final Clock clock = TestClock.now();
        final DefaultSenderHeadersInterceptor testee = new DefaultSenderHeadersInterceptor(
                new SynapseProperties("some name"),
                EnumSet.allOf(Capability.class), clock);

        // when
        final Message<String> message = testee.addDefaultHeaders(Message.message("foo", "bar"));

        // then
        assertThat(message.getHeader().getAll().keySet(), containsInAnyOrder(
                MSG_ID.key(),
                MSG_SENDER.key(),
                MSG_SENDER_TS.key()));
        assertThat(message.getHeader().getAsInstant(MSG_SENDER_TS), is(clock.instant()));
        assertThat(message.getHeader().getAsString(MSG_ID), is(notNullValue()));
        assertThat(message.getHeader().getAsString(MSG_SENDER), is("some name"));
    }
    @Test
    public void shouldExcludeHeaders() {
        // given
        final Clock clock = TestClock.now();
        final DefaultSenderHeadersInterceptor testee = new DefaultSenderHeadersInterceptor(
                new SynapseProperties("some name"),
                EnumSet.of(Capability.MESSAGE_ID, Capability.SENDER_NAME), clock);

        // when
        final Message<String> message = testee.addDefaultHeaders(Message.message("foo", "bar"));

        // then
        assertThat(message.getHeader().getAll().keySet(), containsInAnyOrder(
                MSG_ID.key(),
                MSG_SENDER.key()));
        assertThat(message.getHeader().getAsString(MSG_ID), is(notNullValue()));
        assertThat(message.getHeader().getAsString(MSG_SENDER), is("some name"));

    }

    @Test
    public void shouldDisableDefaultHeaders() {
        // given
        final Clock clock = TestClock.now();
        SynapseProperties senderProperties = new SynapseProperties("some name");
        senderProperties.getSender().getDefaultHeaders().setEnabled(false);
        final DefaultSenderHeadersInterceptor testee = new DefaultSenderHeadersInterceptor(
                senderProperties,
                EnumSet.allOf(Capability.class), clock);

        // when
        final Message<String> message = testee.addDefaultHeaders(Message.message("foo", "bar"));

        // then
        assertThat(message.getHeader().getAll().keySet(), is(empty()));
    }
}