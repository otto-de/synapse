package de.otto.edison.eventsourcing.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "edison.eventsourcing")
public class EventSourcingProperties {
    private Snapshot snapshot = new Snapshot();
    private ConsumerProcess consumerProcess = new ConsumerProcess();
    private Compaction compaction = new Compaction();

    public Snapshot getSnapshot() {
        return snapshot;
    }

    public void setSnapshot(Snapshot snapshot) {
        this.snapshot = snapshot;
    }

    public ConsumerProcess getConsumerProcess() {
        return consumerProcess;
    }

    public void setConsumerProcess(ConsumerProcess consumerProcess) {
        this.consumerProcess = consumerProcess;
    }

    public Compaction getCompaction() {
        return compaction;
    }

    public void setCompaction(Compaction compaction) {
        this.compaction = compaction;
    }

    public static class Snapshot {
        private boolean enabled = true;
        private String bucketTemplate;

        public String getBucketTemplate() {
            return bucketTemplate;
        }

        public void setBucketTemplate(String bucketTemplate) {
            this.bucketTemplate = bucketTemplate;
        }

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }
    }

    public static class ConsumerProcess {
        private boolean enabled = true;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }
    }

    public static class Compaction {
        private boolean enabled = false;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }
    }
}
