package de.otto.synapse.state;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.dizitart.no2.exceptions.NitriteIOException;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;

import java.util.*;

import static com.google.common.collect.Iterables.getFirst;
import static de.otto.synapse.state.NitriteStateRepository.builder;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;
import static org.slf4j.LoggerFactory.getLogger;

public class NitriteStateRepositoryTest {
    private static final Logger LOG = getLogger(NitriteStateRepositoryTest.class);
    private NitriteStateRepository<SomePojo> repository;

    @Before
    public void setUp() throws Exception {
        repository = builder(SomePojo.class).build();
    }

    @After
    public void tearDown() {
        repository.close();
    }

    @Test(expected = NitriteIOException.class)
    public void shouldCloseRepository() {
        repository.close();
        repository.get("foo");
    }

    @Test
    public void shouldFindByKey() {
        // when
        repository.put("someKey", new SomePojo("A", 1));
        Optional<SomePojo> result = repository.get("someKey");
        // then
        assertTrue(result.isPresent());
        assertThat(result.get(), is(new SomePojo("A", 1)));
    }

    @Test
    public void shouldFindByNotIndexedValue() {
        // when
        repository.put("someKey", new SomePojo("A", 1));
        Collection<SomePojo> result = repository.findBy("someString", "A");
        // then
        assertEquals(1, result.size());
        assertThat(getFirst(result, null), is(new SomePojo("A", 1)));
    }

    @Test
    public void shouldFindByIndexedValue() {
        // given
        repository.close();
        repository = builder(SomePojo.class)
                .withIndexed("someKey")
                .build();
        // when
        repository.put("someKey", new SomePojo("A", 1));
        Collection<SomePojo> result = repository.findBy("someString", "A");
        // then
        assertEquals(1, result.size());
        assertThat(getFirst(result, null), is(new SomePojo("A", 1)));
    }

    @Test
    public void shouldReturnOptionalEmptyForUnknownKey() {
        // given
        // when
        Optional<SomePojo> result = repository.get("someUnknownKey");
        // then
        assertFalse(result.isPresent());
    }

    @Test
    public void shouldRemoveEntry() {
        // given
        repository.put("someKey", new SomePojo("A", 1));
        // when
        repository.remove("someKey");
        Optional<SomePojo> result = repository.get("someKey");
        // then
        assertFalse(result.isPresent());
        assertThat(repository.size(), is(0L));
    }

    @Test
    public void shouldReturnRemovedEntry() {
        // given
        SomePojo inserted = new SomePojo("A", 1);
        repository.put("someKey", inserted);
        // when
        Optional<SomePojo> removed = repository.remove("someKey");
        // then
        assertThat(removed.get(), is(inserted));
    }

    @Test
    public void shouldRemoveMissingEntry() {
        // when
        Optional<SomePojo> removed = repository.remove("someKey");
        // then
        assertFalse(removed.isPresent());
    }

    @Test
    public void shouldClearRepository() {
        // given
        repository.put("someKey", new SomePojo("A", 1));
        // when
        repository.clear();
        // then
        assertThat(repository.size(), is(0L));
    }

    @Test
    public void shouldReturnCorrectEntrySize() {
        // given
        repository.put("someKeyA", new SomePojo("A", 1));
        repository.put("someKeyB", new SomePojo("B", 2));
        repository.put("someKeyC", new SomePojo("C", 3));
        // when
        long resultSize = repository.size();
        // then
        assertThat(resultSize, is(3L));
    }

    @Test
    public void shouldIterateOverKeySet() {
        // given
        NitriteStateRepository<SomePojo> repository = builder(SomePojo.class).build();
        repository.put("someKeyA", new SomePojo("A", 1));
        repository.put("someKeyB", new SomePojo("B", 2));
        repository.put("someKeyC", new SomePojo("C", 3));
        // when
        Set<String> resultKeys = repository.keySet();
        // then
        assertThat(resultKeys, containsInAnyOrder("someKeyA", "someKeyB", "someKeyC"));
    }

    @Test
    public void shouldConsumeAll() {
        // given
        repository.put("someKeyA", new SomePojo("A", 1));
        repository.put("someKeyB", new SomePojo("B", 2));
        repository.put("someKeyC", new SomePojo("C", 3));
        // when
        final Map<String,SomePojo> result = new HashMap<>();
        repository.consumeAll(result::put);
        // then
        assertEquals(result.size(), 3L);
        assertThat(result.get("someKeyA"), is(new SomePojo("A", 1)));
        assertThat(result.get("someKeyB"), is(new SomePojo("B", 2)));
        assertThat(result.get("someKeyC"), is(new SomePojo("C", 3)));
    }

    @Test
    public void shouldComputeEntry() {
        // given
        repository.put("someKeyA", new SomePojo("A", 1));
        // when
        repository.compute("someKeyA", (key, value) -> new SomePojo("A", value.get().someInteger + 1));
        repository.compute("someKeyB", (key, value) -> value.orElse(new SomePojo("B", 42)));
        // then
        assertEquals(repository.get("someKeyA").get().someInteger, 2);
        assertEquals(repository.get("someKeyB").get().someInteger, 42);
    }

    @Test
    @Ignore
    public void performance() {
        int count = 1000000;
        LOG.info("Starting to insert " + count + " objects");
        for (int i = 0; i < count; i++) {
            repository.put("testKey" + i, new SomePojo("s" + i, i));
        }
        LOG.info("Inserted " + count + " objects");
        final long size = repository.size();
        LOG.info("Counted " + size + " entries");
        final Set<String> keys = repository.keySet();
        LOG.info("Retrieved " + keys.size() + " keys");
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class SomePojo {

        public String someString;
        public int someInteger;

        // for json serialization
        SomePojo() {
        }

        SomePojo(String someString, int someInteger) {
            this.someString = someString;
            this.someInteger = someInteger;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SomePojo somePojo = (SomePojo) o;
            return someInteger == somePojo.someInteger &&
                    Objects.equals(someString, somePojo.someString);
        }

        @Override
        public int hashCode() {
            return Objects.hash(someString, someInteger);
        }
    }

}
