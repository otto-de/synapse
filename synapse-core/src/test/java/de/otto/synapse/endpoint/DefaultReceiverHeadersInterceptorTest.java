package de.otto.synapse.endpoint;

import de.otto.synapse.configuration.SynapseProperties;
import de.otto.synapse.message.Message;
import de.otto.synapse.message.TextMessage;
import de.otto.synapse.testsupport.TestClock;
import org.junit.Test;

import java.time.Clock;

import static de.otto.synapse.message.DefaultHeaderAttr.MSG_RECEIVER_TS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class DefaultReceiverHeadersInterceptorTest {

    @Test
    public void shouldAddDefaultHeaders() {
        // given
        final Clock clock = TestClock.now();
        final DefaultReceiverHeadersInterceptor testee = new DefaultReceiverHeadersInterceptor(
                new SynapseProperties("some name"),
                clock
        );

        // when
        final Message<String> message = testee.addDefaultHeaders(TextMessage.of("foo", "bar"));

        // then
        assertThat(message.getHeader().getAll().keySet(), containsInAnyOrder(
                MSG_RECEIVER_TS.key()));
        assertThat(message.getHeader().getAsInstant(MSG_RECEIVER_TS), is(clock.instant()));
    }

    @Test
    public void shouldDisableDefaultHeaders() {
        // given
        final Clock clock = TestClock.now();
        SynapseProperties receiverProperties = new SynapseProperties("some name");
        receiverProperties.getReceiver().getDefaultHeaders().setEnabled(false);
        final DefaultReceiverHeadersInterceptor testee = new DefaultReceiverHeadersInterceptor(
                receiverProperties,
                clock
        );

        // when
        final Message<String> message = testee.addDefaultHeaders(TextMessage.of("foo", "bar"));

        // then
        assertThat(message.getHeader().getAll().keySet(), is(empty()));
    }
}