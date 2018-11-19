package de.otto.synapse.configuration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "synapse")
public class SynapseProperties {
    private ConsumerProcess consumerProcess = new ConsumerProcess();
    private Sender sender = new Sender();

    public ConsumerProcess getConsumerProcess() {
        return consumerProcess;
    }

    public void setConsumerProcess(ConsumerProcess consumerProcess) {
        this.consumerProcess = consumerProcess;
    }

    public Sender getSender() {
        return sender;
    }

    public void setSender(Sender sender) {
        this.sender = sender;
    }

    public static class Sender {
        /**
         * The name of the message-sending service. By default, this is the same property
         * 'spring.application.name'.
         *
         * If synapse.sender.default-headers are enabled, messages will be sent with a header attribute
         * 'synapse_msg_sender', containing the value of this property.
         */
        @Value("${spring.application.name}")
        private String name = "Synapse";
        private DefaultHeaders defaultHeaders = new DefaultHeaders();

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public DefaultHeaders getDefaultHeaders() {
            return defaultHeaders;
        }

        public void setDefaultHeaders(DefaultHeaders defaultHeaders) {
            this.defaultHeaders = defaultHeaders;
        }

        public static class DefaultHeaders {
            private boolean enabled = true;

            public boolean isEnabled() {
                return enabled;
            }

            public void setEnabled(boolean enabled) {
                this.enabled = enabled;
            }
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
