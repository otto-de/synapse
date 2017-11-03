package de.otto.edison.eventsourcing;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

@ConfigurationProperties(prefix = "edison.eventsourcing")
public class EventSourcingProperties {
    private String snapshotBucketTemplate;
    private List<String> streamNames;

    public String getSnapshotBucketTemplate() {
        return snapshotBucketTemplate;
    }

    public void setSnapshotBucketTemplate(String snapshotBucketTemplate) {
        this.snapshotBucketTemplate = snapshotBucketTemplate;
    }

    public List<String> getStreamNames() {
        return streamNames;
    }

    public void setStreamNames(List<String> streamNames) {
        this.streamNames = streamNames;
    }
}
