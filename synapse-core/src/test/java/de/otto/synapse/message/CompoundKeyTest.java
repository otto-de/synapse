package de.otto.synapse.message;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class CompoundKeyTest {

    @Test
    public void shouldHavePartitionAndCompactionKey() {
        CompoundKey key = new CompoundKey("foo", "bar");
        assertThat(key.partitionKey(), is("foo"));
        assertThat(key.compactionKey(), is("bar"));
    }

    @Test
    public void shouldImplementToString() {
        CompoundKey key = new CompoundKey("foo", "bar");
        assertThat(key.toString(), is("foo:bar"));
    }

    @Test
    public void shouldImplementEqualsAndHashCode() {
        CompoundKey firstFoo = new CompoundKey("foo", "bar");
        CompoundKey secondFoo = new CompoundKey("foo", "bar");
        CompoundKey bar = new CompoundKey("bar", "barbar");
        assertThat(firstFoo, is(secondFoo));
        assertThat(secondFoo, is(firstFoo));
        assertThat(firstFoo.hashCode(), is(secondFoo.hashCode()));
        assertThat(firstFoo, is(not(bar)));
        assertThat(bar, is(not(firstFoo)));
        assertThat(firstFoo.hashCode(), is(not(bar.hashCode())));
    }
}