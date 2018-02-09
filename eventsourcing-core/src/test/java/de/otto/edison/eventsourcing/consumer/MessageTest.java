package de.otto.edison.eventsourcing.consumer;

import de.otto.edison.eventsourcing.message.Message;
import org.junit.Test;

import java.time.Instant;

import static de.otto.edison.eventsourcing.message.Message.message;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class MessageTest {

    @Test
    public void shouldBuildDefaultEvent() {
        final Instant now = Instant.now();
        final Message<String> message = Message.message(
                "42",
                "ßome dätä",
                "00001",
                now);
        assertThat(message.getKey(), is("42"));
        assertThat(message.getPayload(), is("ßome dätä"));
        assertThat(message.getHeader().getArrivalTimestamp(), is(now));
        assertThat(message.getHeader().getSequenceNumber(), is("00001"));
    }
}
