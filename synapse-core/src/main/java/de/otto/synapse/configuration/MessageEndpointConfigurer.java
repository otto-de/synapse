package de.otto.synapse.configuration;

import de.otto.synapse.endpoint.AbstractMessageEndpoint;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;

/**
 * A configurer used to configure {@link AbstractMessageEndpoint message endpoints}.
 * <p>
 *     Configurations may implement this interface in order to configure message endpoints. For example:
 * </p>
 * <pre><code>
 *     public class ExampleConfiguration implements MessageEndpointConfigurer {
 *
 *          private static final Logger LOG = getLogger(ExampleConfiguration.class);
 *
 *          &#64;Override
 *          public void configureMessageInterceptors(final MessageInterceptorRegistry registry) {
 *
 *              registry.register(receiverChannelsWith((m) -&gt; {
 *                  LOG.info("[receiver] Intercepted message " + m);
 *                  return m;
 *              }));
 *
 *              registry.register(senderChannelsWith((m) -&gt; {
 *                  LOG.info("[sender] Intercepted message " + m);
 *                  return m;
 *              }));
 *
 *          }
 *
 *          // ...
 *     }
 * </code></pre>
 */
public interface MessageEndpointConfigurer {

    /**
     *
     * Registers {@link de.otto.synapse.endpoint.MessageInterceptor message interceptors} used to intercept messages
     * at the sender- and/or receiver-side.
     *
     * @param registry MessageInterceptorRegistry used to register the interceptors.
     */
    default void configureMessageInterceptors(final MessageInterceptorRegistry registry) {};
}
