package de.otto.synapse.state;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static de.otto.synapse.state.ChronicleMapStateRepository.builder;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class ChronicleMapStateRepositoryTest {

    @Test
    public void shouldRetrieveValueAfterPut() {
        // given
        ChronicleMapStateRepository<SomePojo> repository = builder(SomePojo.class).build();
        // when
        repository.put("someKey", new SomePojo("A", 1));
        Optional<SomePojo> result = repository.get("someKey");
        // then
        Assert.assertTrue(result.isPresent());
        assertThat(result.get(), is(new SomePojo("A", 1)));
    }

    @Test
    public void shouldReturnOptionalEmptyForUnknownKey() {
        // given
        ChronicleMapStateRepository<SomePojo> repository = builder(SomePojo.class).build();
        // when
        Optional<SomePojo> result = repository.get("someUnknownKey");
        // then
        Assert.assertTrue(!result.isPresent());
    }

    @Test
    public void shouldReturnOptionalEmptyForRemovedEntry() {
        // given
        ChronicleMapStateRepository<SomePojo> repository = builder(SomePojo.class).build();
        repository.put("someKey", new SomePojo("A", 1));
        // when
        repository.remove("someKey");
        Optional<SomePojo> result = repository.get("someKey");
        // then
        Assert.assertTrue(!result.isPresent());
    }

    @Test
    public void shouldReturnCorrectEntrySize() {
        // given
        ChronicleMapStateRepository<SomePojo> repository = builder(SomePojo.class).build();
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
        ChronicleMapStateRepository<SomePojo> repository = builder(SomePojo.class).build();
        repository.put("someKeyA", new SomePojo("A", 1));
        repository.put("someKeyB", new SomePojo("B", 2));
        repository.put("someKeyC", new SomePojo("C", 3));
        // when
        List<String> resultKeys = new ArrayList<>(repository.keySet());
        // then
        assertThat(resultKeys, containsInAnyOrder("someKeyA", "someKeyB", "someKeyC"));
    }

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
