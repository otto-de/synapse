package de.otto.synapse.message;

import javax.annotation.Nonnull;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class SimpleKey implements Key {

    private static final long serialVersionUID = 5169912180358849391L;

    private final String key;

    SimpleKey(final @Nonnull String key) {
        this.key = requireNonNull(key);
    }

    @Override
    public String partitionKey() {
        return key;
    }

    @Override
    public String compactionKey() {
        return key;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SimpleKey simpleKey = (SimpleKey) o;
        return Objects.equals(key, simpleKey.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key);
    }

    @Override
    public String toString() {
        return key;
    }
}
