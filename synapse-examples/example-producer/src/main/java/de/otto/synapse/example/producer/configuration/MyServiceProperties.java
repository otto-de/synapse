package de.otto.synapse.example.producer.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "exampleservice")
public class MyServiceProperties {
    private String productChannelName;
    private String bananaChannelName;

    public String getProductChannelName() {
        return productChannelName;
    }

    public MyServiceProperties setProductChannelName(String productChannelName) {
        this.productChannelName = productChannelName;
        return this;
    }

}
