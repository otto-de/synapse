package de.otto.synapse.edison.health;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

@ConfigurationProperties(prefix = "synapse.edison.health.startup")
public class HealthProperties {

    private List<String> channels = new ArrayList<>();

    public List<String> getChannels() {
        return channels;
    }

    public void setChannels(List<String> channels) {
        this.channels = channels;
    }
}
