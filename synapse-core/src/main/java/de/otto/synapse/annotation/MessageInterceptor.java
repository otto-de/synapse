package de.otto.synapse.annotation;

import de.otto.synapse.configuration.SynapseAutoConfiguration;
import de.otto.synapse.endpoint.EndpointType;
import de.otto.synapse.message.Message;
import de.otto.synapse.message.TextMessage;

import java.lang.annotation.*;

/**
 * An annotation that marks a method to be used as a {@link de.otto.synapse.endpoint.MessageInterceptor}.
 *
 * <p>The annotated method must expect a single argument with parameter type
 * {@link TextMessage}, or {@link Message Message&lt;String&gt;}. The return type can either be
 * {@code Textmessage}, {@code Message&lt;String&gt;} or void.
 *
 * <p>{@code MessageInterceptors} can be used as {@link de.otto.synapse.endpoint.MessageFilter}: If
 * the annotated method is returning {@code null}, the intercepted message will be dropped, without
 * sending and/or processing it.</p>
 *
 * <p>Processing of {@code @MessageInterceptor} annotations is performed by
 * a {@link MessageInterceptorBeanPostProcessor} that is auto-configured in
 * {@link SynapseAutoConfiguration}.
 *
 * @see TextMessage
 * @see de.otto.synapse.endpoint.MessageInterceptor
 * @see MessageInterceptorBeanPostProcessor
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface MessageInterceptor {

    /**
     * The optional regexp pattern used to select channels by name.
     *
     * <p>By default, all channels will be intercepted.</p>
     *
     * @return regexp
     */
    String channelNamePattern() default ".*";

    /**
     * The optional {@code EndpointType}(s) that will be intercepted.
     *
     * <p>By default, both {@link EndpointType#SENDER} and {@link EndpointType#RECEIVER} endpoints will
     * be intercepted.</p>
     *
     * @return intercepted {@code EndpointType}(s)
     */
    EndpointType[] endpointType() default {EndpointType.SENDER, EndpointType.RECEIVER};

}
