package de.otto.edison.eventsourcing.example.consumer.payload;

public class ProductPayload {

    private String id;
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
