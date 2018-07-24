package de.otto.synapse.annotation.messagequeue;

import de.otto.synapse.endpoint.receiver.MessageQueueReceiverEndpoint;
import de.otto.synapse.message.Message;

import java.lang.annotation.*;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface MessageQueueConsumer {

    String endpointName();

    String keyPattern() default ".*";

    /**
     * The type of the {@link Message#getPayload()} produced by the {@link MessageQueueReceiverEndpoint}
     *
     * @return payload type
     */
    Class<?> payloadType();
}
