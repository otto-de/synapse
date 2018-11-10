package de.otto.synapse.configuration.aws;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "synapse.snapshot")
public class SnapshotProperties {

    private String bucketName = null;

    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(String bucketName) {
            this.bucketName = bucketName;
        }

}
