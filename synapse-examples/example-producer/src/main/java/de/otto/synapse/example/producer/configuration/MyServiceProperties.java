package de.otto.synapse.example.producer.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "exampleservice")
public class MyServiceProperties {
    private String productStreamName;
    private String bananaStreamName;

    public String getProductStreamName() {
        return productStreamName;
    }

    public MyServiceProperties setProductStreamName(String productStreamName) {
        this.productStreamName = productStreamName;
        return this;
    }

}
