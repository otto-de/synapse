package de.otto.edison.eventsourcing.example.consumer.payload;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ProductPayload {

    @JsonProperty
    private String id;
    @JsonProperty
    private long price;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getPrice() {
        return price;
    }

    public void setPrice(long price) {
        this.price = price;
    }
}
