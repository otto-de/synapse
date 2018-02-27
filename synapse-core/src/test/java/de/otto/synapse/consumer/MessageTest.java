package de.otto.synapse.consumer;

import de.otto.synapse.message.Message;
import org.junit.Test;

import java.time.Instant;

import static de.otto.synapse.message.Header.responseHeader;
import static de.otto.synapse.message.Message.message;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class MessageTest {

    @Test
    public void shouldBuildDefaultEvent() {
        final Instant now = Instant.now();
        final Message<String> message = message(
                "42",
                responseHeader("00001", now, null),
                "ßome dätä"
        );
        assertThat(message.getKey(), is("42"));
        assertThat(message.getPayload(), is("ßome dätä"));
        assertThat(message.getHeader().getArrivalTimestamp(), is(now));
        assertThat(message.getHeader().getSequenceNumber(), is("00001"));
    }
}
