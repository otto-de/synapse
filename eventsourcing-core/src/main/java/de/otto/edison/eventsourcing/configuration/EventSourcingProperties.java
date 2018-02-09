package de.otto.edison.eventsourcing.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "edison.eventsourcing")
public class EventSourcingProperties {
    private ConsumerProcess consumerProcess = new ConsumerProcess();

    public ConsumerProcess getConsumerProcess() {
        return consumerProcess;
    }

    public void setConsumerProcess(ConsumerProcess consumerProcess) {
        this.consumerProcess = consumerProcess;
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
