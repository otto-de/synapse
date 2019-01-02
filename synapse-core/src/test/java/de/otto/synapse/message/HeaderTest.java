package de.otto.synapse.message;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.time.Instant;
import java.util.Optional;

import static de.otto.synapse.message.Header.of;
import static java.time.Instant.now;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class HeaderTest {

    @Test
    public void shouldReturnInstantAttr() {
        final Instant time = now();
        final Header header = of(ImmutableMap.of("ts", time.toString()));
        assertThat(header.getAsInstant("ts"), is(time));
    }

    @Test
    public void shouldReturnNullInstantAttr() {
        final Header header = of(ImmutableMap.of());
        assertThat(header.getAsInstant("ts"), is(nullValue()));
    }

    @Test
    public void shouldReturnStringAttr() {
        final Header header = of(ImmutableMap.of("x", "foobar"));
        assertThat(header.getAsString("x"), is("foobar"));
    }

    @Test
    public void shouldReturnNullStringAttr() {
        final Header header = of(ImmutableMap.of());
        assertThat(header.getAsString("x"), is(nullValue()));
    }

    @Test
    public void shouldReturnEmptyPosition() {
        final Header header = of(ImmutableMap.of());
        assertThat(header.getShardPosition(), is(Optional.empty()));
    }
}