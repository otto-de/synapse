package de.otto.synapse.messagestore;

import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class IndexTest {

    @Test
    public void shouldReturnName() {
        assertThat(Index.PARTITION_KEY.name(), is("partitionKey"));
    }

    @Test
    public void shouldReturnFieldName() {
        assertThat(Index.PARTITION_KEY.name(), is("partitionKey"));
    }

    @Test
    public void shouldSupportValueOf() {
        assertThat(Index.valueOf("partitionKey"), is(Index.PARTITION_KEY));
    }
}