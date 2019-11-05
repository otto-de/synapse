package de.otto.synapse.example.consumer.interceptor;

import de.otto.synapse.annotation.MessageInterceptor;
import de.otto.synapse.endpoint.EndpointType;
import de.otto.synapse.message.Message;
import org.slf4j.Logger;
import org.springframework.stereotype.Component;

import static org.slf4j.LoggerFactory.getLogger;

@Component
public class ExampleIterceptor {
    private static final Logger LOG = getLogger(ExampleIterceptor.class);

    @MessageInterceptor(endpointType = EndpointType.SENDER)
    public void testSenderInterceptor(final Message<String> message) {
        LOG.info("[sender] Intercepted message {}", message);
    }

    @MessageInterceptor(endpointType = EndpointType.RECEIVER)
    public void testReceiverInterceptor(final Message<String> message) {
        LOG.info("[receiver] Intercepted message {}", message);
    }
}
