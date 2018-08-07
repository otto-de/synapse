package de.otto.synapse.annotation.messagequeue;

import de.otto.synapse.endpoint.receiver.DelegateMessageQueueReceiverEndpoint;
import org.slf4j.Logger;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.util.MultiValueMap;

import java.util.Objects;

import static com.google.common.base.Strings.emptyToNull;
import static de.otto.synapse.annotation.BeanNameHelper.beanNameForMessageQueue;
import static de.otto.synapse.annotation.BeanNameHelper.beanNameForMessageQueueReceiverEndpoint;
import static java.lang.String.format;
import static org.slf4j.LoggerFactory.getLogger;
import static org.springframework.beans.factory.support.AbstractBeanDefinition.DEPENDENCY_CHECK_ALL;
import static org.springframework.beans.factory.support.BeanDefinitionBuilder.genericBeanDefinition;

/**
 * {@link ImportBeanDefinitionRegistrar} for message log support.
 *
 * @see EnableMessageQueueReceiverEndpoint
 */
public class MessageQueueReceiverEndpointBeanRegistrar implements ImportBeanDefinitionRegistrar, EnvironmentAware {

    private static final Logger LOG = getLogger(MessageQueueReceiverEndpointBeanRegistrar.class);

    private Environment environment;

    /**
     * Set the {@code Environment} that this component runs in.
     *
     * @param environment the current Spring environment
     */
    @Override
    public void setEnvironment(final Environment environment) {
        this.environment = environment;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void registerBeanDefinitions(final AnnotationMetadata metadata,
                                        final BeanDefinitionRegistry registry) {
        /*
        @EnableMessageQueueReceiverEndpoint is a @Repeatable annotation. If there are multiple annotations present,
        there is an automagically added @EnableMessageQueueReceiverEndpoints annotation, containing the @EnableMessageQueueReceiverEndpoint
        annotations as value.
         */
        final MultiValueMap<String, Object> messageQueuesAttr = metadata.getAllAnnotationAttributes(EnableMessageQueueReceiverEndpoints.class.getName(), false);
        if (messageQueuesAttr != null) {
            final Object value = messageQueuesAttr.getFirst("value");
            registerMultipleMessageQueueReceiverEndpoints(registry, (AnnotationAttributes[]) value);
        } else {
            final MultiValueMap<String, Object> messageQueueAttr = metadata.getAllAnnotationAttributes(EnableMessageQueueReceiverEndpoint.class.getName(), false);
            registerSingleMessageQueue(registry, messageQueueAttr);
        }

    }

    private void registerMultipleMessageQueueReceiverEndpoints(final BeanDefinitionRegistry registry,
                                                               final AnnotationAttributes[] annotationAttributesArr) {
        for (final AnnotationAttributes annotationAttributes : annotationAttributesArr) {
            final String channelName = environment.resolvePlaceholders(annotationAttributes.getString("channelName"));
            final String messageQueueBeanName = Objects.toString(
                    emptyToNull(annotationAttributes.getString("name")),
                    beanNameForMessageQueue(channelName));
            final String messageQueueReceiverEndpointBeanName = Objects.toString(
                    emptyToNull(annotationAttributes.getString("messageQueueReceiverEndpoint")),
                    beanNameForMessageQueueReceiverEndpoint(channelName));
            if (!registry.containsBeanDefinition(messageQueueReceiverEndpointBeanName)) {
                registerMessageQueueReceiverEndpointBeanDefinition(registry, messageQueueBeanName, channelName);
            } else {
                throw new BeanCreationException(messageQueueReceiverEndpointBeanName, format("messageQueueReceiverEndpoint %s is already registered.", messageQueueReceiverEndpointBeanName));
            }
        }
    }

    private void registerSingleMessageQueue(final BeanDefinitionRegistry registry,
                                            final MultiValueMap<String, Object> messageQueueAttr) {
        if (messageQueueAttr != null) {

            final String channelName = environment.resolvePlaceholders(
                    messageQueueAttr.getFirst("channelName").toString());

            final String messageQueueBeanName = Objects.toString(
                    emptyToNull(messageQueueAttr.getFirst("name").toString()),
                    beanNameForMessageQueue(channelName));

            final String messageQueueReceiverEndpointBeanName = Objects.toString(
                    emptyToNull(messageQueueAttr.getFirst("messageQueueReceiverEndpoint").toString()),
                    beanNameForMessageQueueReceiverEndpoint(channelName));

            if (!registry.containsBeanDefinition(messageQueueReceiverEndpointBeanName)) {
                registerMessageQueueReceiverEndpointBeanDefinition(registry, messageQueueBeanName, channelName);
            } else {
                throw new BeanCreationException(messageQueueReceiverEndpointBeanName, format("MessageQueueReceiverEndpoint %s is already registered.", messageQueueReceiverEndpointBeanName));
            }
        }
    }

    private void registerMessageQueueReceiverEndpointBeanDefinition(final BeanDefinitionRegistry registry,
                                                                    final String beanName,
                                                                    final String channelName) {


        registry.registerBeanDefinition(
                beanName,
                genericBeanDefinition(DelegateMessageQueueReceiverEndpoint.class)
                        .addConstructorArgValue(channelName)
                        .setDependencyCheck(DEPENDENCY_CHECK_ALL)
                        .getBeanDefinition()
        );

        LOG.info("Registered MessageQueueReceiverEndpoint {} with for channelName {}", beanName, channelName);
    }
}
