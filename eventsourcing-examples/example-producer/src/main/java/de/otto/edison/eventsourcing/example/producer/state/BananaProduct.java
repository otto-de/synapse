package de.otto.edison.eventsourcing.example.consumer.state;

public class BananaProduct {
    private final String id;
    private final String color;
    private final long price;

    public String getId() {
        return id;
    }

    public String getColor() {
        return color;
    }

    public long getPrice() {
        return price;
    }

    private BananaProduct(Builder builder) {
        id = builder.id;
        color = builder.color;
        price = builder.price;
    }

    public static Builder bananaProductBuilder() {
        return new Builder();
    }

    public static Builder bananaProductBuilder(BananaProduct copy) {
        Builder builder = new Builder();
        builder.id = copy.getId();
        builder.color = copy.getColor();
        builder.price = copy.getPrice();
        return builder;
    }


    public static final class Builder {
        private String id;
        private String color;
        private long price;

        private Builder() {
        }

        public Builder withId(String val) {
            id = val;
            return this;
        }

        public Builder withColor(String val) {
            color = val;
            return this;
        }

        public Builder withPrice(long val) {
            price = val;
            return this;
        }

        public BananaProduct build() {
            return new BananaProduct(this);
        }
    }
}
