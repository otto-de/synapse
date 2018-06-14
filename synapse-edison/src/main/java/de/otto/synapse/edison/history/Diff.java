package de.otto.synapse.edison.history;

import javax.annotation.Nullable;
import java.util.Objects;

public class Diff {
    private final String key;
    private final Object previousValue;
    private final Object newValue;

    public Diff(final String key,
                final Object previousValue,
                final Object newValue) {
        this.key = key;
        this.previousValue = previousValue;
        this.newValue = newValue;
    }

    public String getKey() {
        return key;
    }

    @Nullable
    public Object getPreviousValue() {
        return previousValue;
    }

    @Nullable
    public Object getNewValue() {
        return newValue;
    }

    public boolean isEqual() {
        return Objects.equals(previousValue, newValue);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Diff diff = (Diff) o;
        return Objects.equals(key, diff.key) &&
                Objects.equals(previousValue, diff.previousValue) &&
                Objects.equals(newValue, diff.newValue);
    }

    @Override
    public int hashCode() {

        return Objects.hash(key, previousValue, newValue);
    }

    @Override
    public String toString() {
        return "Diff{" +
                "key='" + key + '\'' +
                ", previousValue=" + previousValue +
                ", newValue=" + newValue +
                '}';
    }
}
