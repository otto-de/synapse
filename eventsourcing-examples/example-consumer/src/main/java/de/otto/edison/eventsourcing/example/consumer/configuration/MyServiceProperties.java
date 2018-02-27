package de.otto.edison.eventsourcing.example.consumer.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "exampleservice")
public class MyServiceProperties {
    private String productChannel;
    private String bananaChannel;

    public String getProductChannel() {
        return productChannel;
    }

    public MyServiceProperties setProductChannel(String productChannel) {
        this.productChannel = productChannel;
        return this;
    }

    public String getBananaChannel() {
        return bananaChannel;
    }

    public MyServiceProperties setBananaChannel(String bananaChannel) {
        this.bananaChannel = bananaChannel;
        return this;
    }
}
