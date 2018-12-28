package de.otto.synapse.message;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class SimpleKeyTest {

    @Test
    public void shouldHaveSamePartitionAndCompactionKey() {
        SimpleKey key = new SimpleKey("foo");
        assertThat(key.partitionKey(), is(key.compactionKey()));
    }

    @Test
    public void shouldImplementToString() {
        SimpleKey key = new SimpleKey("foo");
        assertThat(key.toString(), is("foo"));
    }

    @Test
    public void shouldImplementEqualsAndHashCode() {
        SimpleKey firstFoo = new SimpleKey("foo");
        SimpleKey secondFoo = new SimpleKey("foo");
        SimpleKey bar = new SimpleKey("bar");
        assertThat(firstFoo, is(secondFoo));
        assertThat(secondFoo, is(firstFoo));
        assertThat(firstFoo.hashCode(), is(secondFoo.hashCode()));
        assertThat(firstFoo, is(not(bar)));
        assertThat(bar, is(not(firstFoo)));
        assertThat(firstFoo.hashCode(), is(not(bar.hashCode())));
    }
}