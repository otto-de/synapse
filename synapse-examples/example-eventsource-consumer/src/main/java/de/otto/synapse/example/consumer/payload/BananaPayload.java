package de.otto.synapse.example.consumer.payload;


import com.fasterxml.jackson.annotation.JsonProperty;

public class BananaPayload {

    @JsonProperty
    private String id;
    @JsonProperty
    private String color;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getColor() {
        return color;
    }

    public void setColor(String color) {
        this.color = color;
    }

    @Override
    public String toString() {
        return "BananaPayload{" +
                "id='" + id + '\'' +
                ", color='" + color + '\'' +
                '}';
    }
}
