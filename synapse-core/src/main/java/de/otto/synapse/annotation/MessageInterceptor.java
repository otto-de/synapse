package de.otto.synapse.annotation;

import de.otto.synapse.endpoint.EndpointType;

import java.lang.annotation.*;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface MessageInterceptor {

    String channelNamePattern() default ".*";

    EndpointType[] endpointType() default {EndpointType.SENDER, EndpointType.RECEIVER};

}
