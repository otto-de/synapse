package de.otto.synapse.annotation.messagequeue;

import de.otto.synapse.endpoint.sender.DelegateMessageQueueSenderEndpoint;
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
import static de.otto.synapse.annotation.BeanNameHelper.beanNameForMessageQueueSenderEndpoint;
import static java.lang.String.format;
import static org.slf4j.LoggerFactory.getLogger;
import static org.springframework.beans.factory.support.AbstractBeanDefinition.DEPENDENCY_CHECK_ALL;
import static org.springframework.beans.factory.support.BeanDefinitionBuilder.genericBeanDefinition;

/**
 * {@link ImportBeanDefinitionRegistrar} for message log support.
 *
 * @see EnableMessageQueueReceiverEndpoint
 */
public class MessageQueueSenderEndpointBeanRegistrar implements ImportBeanDefinitionRegistrar, EnvironmentAware {

    private static final Logger LOG = getLogger(MessageQueueSenderEndpointBeanRegistrar.class);

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
        @EnableMessageQueueSenderEndpoint is a @Repeatable annotation. If there are multiple annotations present,
        there is an automagically added @EnableMessageQueueSenderEndpoints annotation, containing the
        @EnableMessageQueueSenderEndpoint annotations as value.
         */
        final MultiValueMap<String, Object> messageQueuesAttr = metadata.getAllAnnotationAttributes(EnableMessageQueueSenderEndpoints.class.getName(), false);
        if (messageQueuesAttr != null) {
            final Object value = messageQueuesAttr.getFirst("value");
            registerMultipleMessageQueueSenderEndpoints(registry, (AnnotationAttributes[]) value);
        } else {
            final MultiValueMap<String, Object> messageQueueAttr = metadata.getAllAnnotationAttributes(EnableMessageQueueSenderEndpoint.class.getName(), false);
            registerSingleMessageQueueSenderEndpoint(registry, messageQueueAttr);
        }

    }

    private void registerMultipleMessageQueueSenderEndpoints(final BeanDefinitionRegistry registry,
                                                             final AnnotationAttributes[] annotationAttributesArr) {
        for (final AnnotationAttributes annotationAttributes : annotationAttributesArr) {
            final String channelName = environment.resolvePlaceholders(annotationAttributes.getString("channelName"));
            final String messageQueueSenderEndpointBeanName = Objects.toString(
                    emptyToNull(annotationAttributes.getString("name")),
                    beanNameForMessageQueueSenderEndpoint(channelName));
            if (!registry.containsBeanDefinition(messageQueueSenderEndpointBeanName)) {
                registerMessageQueueSenderEndpointBeanDefinition(registry, messageQueueSenderEndpointBeanName, channelName);
            } else {
                throw new BeanCreationException(messageQueueSenderEndpointBeanName, format("messageQueueSenderEndpoint %s is already registered.", messageQueueSenderEndpointBeanName));
            }
        }
    }

    private void registerSingleMessageQueueSenderEndpoint(final BeanDefinitionRegistry registry,
                                                          final MultiValueMap<String, Object> messageQueueAttr) {
        if (messageQueueAttr != null) {

            final String channelName = environment.resolvePlaceholders(
                    messageQueueAttr.getFirst("channelName").toString());

            final String messageQueueSenderEndpointBeanName = Objects.toString(
                    emptyToNull(messageQueueAttr.getFirst("name").toString()),
                    beanNameForMessageQueueSenderEndpoint(channelName));

            if (!registry.containsBeanDefinition(messageQueueSenderEndpointBeanName)) {
                registerMessageQueueSenderEndpointBeanDefinition(registry, messageQueueSenderEndpointBeanName, channelName);
            } else {
                throw new BeanCreationException(messageQueueSenderEndpointBeanName, format("MessageQueueReceiverEndpoint %s is already registered.", messageQueueSenderEndpointBeanName));
            }
        }
    }

    private void registerMessageQueueSenderEndpointBeanDefinition(final BeanDefinitionRegistry registry,
                                                                  final String beanName,
                                                                  final String channelName) {


        registry.registerBeanDefinition(
                beanName,
                genericBeanDefinition(DelegateMessageQueueSenderEndpoint.class)
                        .addConstructorArgValue(channelName)
                        .setDependencyCheck(DEPENDENCY_CHECK_ALL)
                        .getBeanDefinition()
        );

        LOG.info("Registered MessageQueueSenderEndpoint {} with for channelName {}", beanName, channelName);
    }
}
