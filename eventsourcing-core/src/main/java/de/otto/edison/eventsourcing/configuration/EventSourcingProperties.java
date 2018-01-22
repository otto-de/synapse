package de.otto.edison.eventsourcing.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "edison.eventsourcing")
public class EventSourcingProperties {
    private Snapshot snapshot = new Snapshot();
    private ConsumerProcess consumerProcess = new ConsumerProcess();

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


    public static class Snapshot {
        private String bucketName;
        private String sseType;
        private String sseKey;

        public String getBucketName() {
            return bucketName;
        }

        public void setBucketName(String bucketName) {
            this.bucketName = bucketName;
        }

        public String getSseKey() {
            return sseKey;
        }

        public Snapshot setSseKey(String sseKey) {
            this.sseKey = sseKey;
            return this;
        }

        public String getSseType() {
            return sseType;
        }

        public Snapshot setSseType(String sseType) {
            this.sseType = sseType;
            return this;
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

}
