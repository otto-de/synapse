package de.otto.edison.eventsourcing.consumer;

import org.junit.Test;

import java.time.Instant;

import static de.otto.edison.eventsourcing.consumer.Event.event;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class EventTest {

    @Test
    public void shouldBuildDefaultEvent() {
        final Instant now = Instant.now();
        final Event<String> event = event(
                "42",
                "ßome dätä",
                "00001",
                now);
        assertThat(event.key(), is("42"));
        assertThat(event.payload(), is("ßome dätä"));
        assertThat(event.arrivalTimestamp(), is(now));
        assertThat(event.sequenceNumber(), is("00001"));
    }
}
