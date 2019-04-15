package de.otto.synapse.message;

import javax.annotation.Nonnull;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class CompoundKey implements Key {

    private static final long serialVersionUID = -39690651225375664L;

    private final String partitionKey;
    private final String compactionKey;

    CompoundKey(final @Nonnull String partitionKey,
                        final @Nonnull String compactionKey) {
        this.partitionKey = requireNonNull(partitionKey);
        this.compactionKey = requireNonNull(compactionKey);
    }

    @Override
    public String partitionKey() {
        return partitionKey;
    }

    @Override
    public String compactionKey() {
        return compactionKey;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CompoundKey that = (CompoundKey) o;
        return Objects.equals(partitionKey, that.partitionKey) &&
                Objects.equals(compactionKey, that.compactionKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitionKey, compactionKey);
    }

    @Override
    public String toString() {
        return partitionKey + ':' + compactionKey;
    }
}
