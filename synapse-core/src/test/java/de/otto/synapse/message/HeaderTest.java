package de.otto.synapse.message;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.time.Instant;
import java.util.Optional;

import static de.otto.synapse.message.Header.requestHeader;
import static java.time.Instant.now;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class HeaderTest {

    @Test
    public void shouldReturnInstantAttr() {
        final Instant time = now();
        final Header header = requestHeader(ImmutableMap.of("ts", time.toString()));
        assertThat(header.getInstantAttribute("ts"), is(time));
    }

    @Test
    public void shouldReturnNullInstantAttr() {
        final Header header = requestHeader(ImmutableMap.of());
        assertThat(header.getInstantAttribute("ts"), is(nullValue()));
    }

    @Test
    public void shouldReturnStringAttr() {
        final Header header = requestHeader(ImmutableMap.of("x", "foobar"));
        assertThat(header.getStringAttribute("x"), is("foobar"));
    }

    @Test
    public void shouldReturnNullStringAttr() {
        final Header header = requestHeader(ImmutableMap.of());
        assertThat(header.getStringAttribute("x"), is(nullValue()));
    }

    @Test
    public void shouldReturnEmptyPosition() {
        final Header header = requestHeader(ImmutableMap.of());
        assertThat(header.getShardPosition(), is(Optional.empty()));
    }
}