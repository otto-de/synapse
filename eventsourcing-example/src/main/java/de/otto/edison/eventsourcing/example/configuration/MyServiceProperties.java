package de.otto.edison.eventsourcing.example.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "exampleservice")
public class MyServiceProperties {
    private String productStreamName = "product-stream";
    private String bananaStreamName = "banana-stream";

    public String getProductStreamName() {
        return productStreamName;
    }

    public MyServiceProperties setProductStreamName(String productStreamName) {
        this.productStreamName = productStreamName;
        return this;
    }

    public String getBananaStreamName() {
        return bananaStreamName;
    }

    public MyServiceProperties setBananaStreamName(String bananaStreamName) {
        this.bananaStreamName = bananaStreamName;
        return this;
    }
}
