package de.otto.synapse.annotation;

import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpoint;
import de.otto.synapse.message.Message;

import java.lang.annotation.*;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface MessageLogConsumer {

    /**
     * The name of the {@link MessageLogReceiverEndpoint} bean from which messages should be consumed.
     *
     * @return bean name
     */
    String endpointName() default "";

    /**
     * The key-pattern used to filter messages by key.
     *
     * @return key pattern
     */
    String keyPattern() default ".*";

    /**
     * The type of the {@link Message#getPayload()} produced by the {@link MessageLogReceiverEndpoint}
     *
     * @return payload type
     */
    Class<?> payloadType();

}
