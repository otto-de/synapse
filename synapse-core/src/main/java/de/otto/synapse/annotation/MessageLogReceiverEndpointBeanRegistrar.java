package de.otto.synapse.annotation;

import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.selector.MessageLog;
import de.otto.synapse.endpoint.receiver.DelegateMessageLogReceiverEndpoint;
import de.otto.synapse.endpoint.receiver.MessageLogConsumerContainer;
import org.slf4j.Logger;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.annotation.AnnotationAttributes;

import java.lang.annotation.Annotation;

import static java.lang.String.format;
import static org.slf4j.LoggerFactory.getLogger;
import static org.springframework.beans.factory.support.AbstractBeanDefinition.DEPENDENCY_CHECK_ALL;
import static org.springframework.beans.factory.support.BeanDefinitionBuilder.genericBeanDefinition;

/**
 * {@link ImportBeanDefinitionRegistrar} for message log support.
 *
 * @see EnableMessageLogReceiverEndpoint
 */
public class MessageLogReceiverEndpointBeanRegistrar extends AbstractAnnotationBasedBeanRegistrar {

    private static final Logger LOG = getLogger(MessageLogReceiverEndpointBeanRegistrar.class);

    @Override
    protected Class<? extends Annotation> getAnnotationType() {
        return EnableMessageLogReceiverEndpoint.class;
    }

    @Override
    protected void registerBeanDefinitions(final String channelName,
                                           final String beanName,
                                           final AnnotationAttributes annotationAttributes,
                                           final BeanDefinitionRegistry registry) {
        final String processorBeanName = beanName + "Processor";

        final Class<? extends MessageLog> channelSelector = annotationAttributes.getClass("selector");

        if (!registry.containsBeanDefinition(beanName)) {
            registerMessageLogReceiverEndpointBeanDefinition(registry, beanName, channelName, channelSelector);
        } else {
            throw new BeanCreationException(beanName, format("MessageLogReceiverEndpoint %s is already registered.", beanName));
        }
        if (!registry.containsBeanDefinition(processorBeanName)) {
            registerMessageLogReceiverEndpointProcessorBeanDefinition(registry, processorBeanName, beanName, channelName);
        } else {
            throw new BeanCreationException(beanName, format("MessageLogReceiverEndpointProcessor %s is already registered.", processorBeanName));
        }
    }

    private void registerMessageLogReceiverEndpointBeanDefinition(final BeanDefinitionRegistry registry,
                                                                  final String beanName,
                                                                  final String channelName,
                                                                  final Class<? extends MessageLog> channelSelector) {


        registry.registerBeanDefinition(
                beanName,
                genericBeanDefinition(DelegateMessageLogReceiverEndpoint.class)
                        .addConstructorArgValue(channelName)
                        .addConstructorArgValue(channelSelector)
                        .setDependencyCheck(DEPENDENCY_CHECK_ALL)
                        .getBeanDefinition()
        );

        LOG.info("Registered MessageLogReceiverEndpoint {} with for channelName {}", beanName, channelName);
    }

    private void registerMessageLogReceiverEndpointProcessorBeanDefinition(final BeanDefinitionRegistry registry,
                                                                           final String beanName,
                                                                           final String messageLogBeanName,
                                                                           final String channelName) {
        registry.registerBeanDefinition(
                beanName,
                genericBeanDefinition(MessageLogConsumerContainer.class)
                        .addConstructorArgValue(messageLogBeanName)
                        // TODO: support other channel positions as a starting point for @EnableMessageLogReceiverEndpoint
                        .addConstructorArgValue(ChannelPosition.fromHorizon())
                        .setDependencyCheck(DEPENDENCY_CHECK_ALL)
                        .getBeanDefinition()
        );
        LOG.info("Registered MessageLogReceiverEndpointProcessor {} with for channelName {}", beanName, channelName);
    }

}
