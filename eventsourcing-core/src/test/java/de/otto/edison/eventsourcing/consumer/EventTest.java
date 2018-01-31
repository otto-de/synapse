package de.otto.edison.eventsourcing.consumer;

import de.otto.edison.eventsourcing.event.Event;
import org.junit.Test;

import java.time.Instant;

import static de.otto.edison.eventsourcing.event.Event.event;
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
        assertThat(event.getEventBody().getKey(), is("42"));
        assertThat(event.getEventBody().getPayload(), is("ßome dätä"));
        assertThat(event.getArrivalTimestamp(), is(now));
        assertThat(event.getSequenceNumber(), is("00001"));
    }
}
