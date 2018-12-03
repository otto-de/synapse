package de.otto.synapse.configuration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "synapse")
public class SynapseProperties {
    private final ConsumerProcess consumerProcess = new ConsumerProcess();
    private final Sender sender;

    public SynapseProperties(@Value("${spring.application.name:Synapse}") String defaultName) {
        this.sender = new Sender(defaultName);
    }

    public ConsumerProcess getConsumerProcess() {
        return consumerProcess;
    }

    public Sender getSender() {
        return sender;
    }

    public static class Sender {

        /**
         * The name of the message-sending service. By default, this is the same property
         * 'spring.application.name'.
         *
         * If synapse.sender.default-headers are enabled, messages will be sent with a header attribute
         * 'synapse_msg_sender', containing the value of this property.
         */
        private String name;

        private final DefaultHeaders defaultHeaders = new DefaultHeaders();

        private Sender(final String defaultName) {
            this.name = defaultName;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public DefaultHeaders getDefaultHeaders() {
            return defaultHeaders;
        }


        public class DefaultHeaders {
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
