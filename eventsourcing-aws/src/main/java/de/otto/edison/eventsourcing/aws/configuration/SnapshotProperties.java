package de.otto.edison.eventsourcing.aws.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "edison.eventsourcing.snapshot")
public class SnapshotProperties {

    private String bucketName = null;

    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(String bucketName) {
            this.bucketName = bucketName;
        }

}
