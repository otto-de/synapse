package de.otto.edison.eventsourcing.s3.local;

import java.util.Arrays;
import java.util.Objects;

public class BucketItem {

    private final String name;
    private final byte[] data;

    public String getName() {
        return name;
    }

    public byte[] getData() {
        return data;
    }

    private BucketItem(Builder builder) {
        name = builder.name;
        data = builder.data;
    }

    public static Builder bucketItemBuilder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BucketItem that = (BucketItem) o;
        return Objects.equals(name, that.name) &&
                Arrays.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, data);
    }

    public static Builder bucketItemBuilder(BucketItem copy) {
        Builder builder = new Builder();
        builder.name = copy.getName();
        builder.data = copy.getData();
        return builder;
    }

    public static final class Builder {
        private String name;
        private byte[] data;

        private Builder() {
        }

        public Builder withName(String val) {
            name = val;
            return this;
        }

        public Builder withData(byte[] val) {
            data = val;
            return this;
        }

        public BucketItem build() {
            return new BucketItem(this);
        }
    }
}
