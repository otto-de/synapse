package de.otto.synapse.message;

import org.junit.Test;

import java.time.Instant;

import static de.otto.synapse.channel.ChannelPosition.of;
import static de.otto.synapse.message.Header.responseHeader;
import static de.otto.synapse.message.Message.message;
import static java.time.Duration.ofMillis;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class MessageTest {

    @Test
    public void shouldBuildEventWithHeader() {
        final Instant now = Instant.now();
        final Message<String> message = message(
                "42",
                responseHeader(of("some-channel", "00001"), now, ofMillis(42L)),
                "ßome dätä"
        );
        assertThat(message.getKey(), is("42"));
        assertThat(message.getPayload(), is("ßome dätä"));
        assertThat(message.getHeader().getArrivalTimestamp(), is(now));
        assertThat(message.getHeader().getChannelPosition().get().positionOf("some-channel"), is("00001"));
        assertThat(message.getHeader().getDurationBehind().get(), is(ofMillis(42L)));
    }

    @Test
    public void shouldBuildEventWithoutHeader() {
        final Instant now = Instant.now();
        final Message<String> message = message(
                "42",
                "ßome dätä"
        );
        assertThat(message.getKey(), is("42"));
        assertThat(message.getPayload(), is("ßome dätä"));
        assertThat(message.getHeader().getArrivalTimestamp().isBefore(now), is(false));
        assertThat(message.getHeader().getDurationBehind().isPresent(), is(false));
        assertThat(message.getHeader().getChannelPosition().isPresent(), is(false));
    }
}
