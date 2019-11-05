package de.otto.synapse.annotation;

import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.endpoint.receiver.DelegateMessageLogReceiverEndpoint;
import de.otto.synapse.endpoint.receiver.MessageLogConsumerContainer;
import org.slf4j.Logger;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.util.MultiValueMap;

import java.util.LinkedHashMap;
import java.util.Objects;

import static com.google.common.base.Strings.emptyToNull;
import static de.otto.synapse.annotation.BeanNameHelper.beanNameForMessageLogReceiverEndpoint;
import static java.lang.String.format;
import static org.slf4j.LoggerFactory.getLogger;
import static org.springframework.beans.factory.support.AbstractBeanDefinition.DEPENDENCY_CHECK_ALL;
import static org.springframework.beans.factory.support.BeanDefinitionBuilder.genericBeanDefinition;

/**
 * {@link ImportBeanDefinitionRegistrar} for message log support.
 *
 * @see EnableMessageLogReceiverEndpoint
 */
public class MessageLogReceiverEndpointBeanRegistrar implements ImportBeanDefinitionRegistrar, EnvironmentAware {

    private static final Logger LOG = getLogger(MessageLogReceiverEndpointBeanRegistrar.class);

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
        @EnableMessageLogReceiverEndpoint is a @Repeatable annotation. If there are multiple annotations present,
        there is an automagically added @EnableMessageLogReceiverEndpoints annotation, containing the @EnableMessageLogReceiverEndpoint
        annotations as value.
         */
        final MultiValueMap<String, Object> messageLogsAttr = metadata.getAllAnnotationAttributes(EnableMessageLogReceiverEndpoints.class.getName(), false);
        if (messageLogsAttr != null) {
            final Object value = messageLogsAttr.getFirst("value");
            if (value == null) {
                return;
            }
            LinkedHashMap[] castedValue = (LinkedHashMap[])value;
            AnnotationAttributes[] attributes = new AnnotationAttributes[castedValue.length];
            for(int i=0; i<castedValue.length; i++) {
                attributes[i] = new AnnotationAttributes(castedValue[i]);
            }
            registerMultipleMessageLogReceiverEndpoints(registry, attributes);
        } else {
            final MultiValueMap<String, Object> messageLogAttr = metadata.getAllAnnotationAttributes(EnableMessageLogReceiverEndpoint.class.getName(), false);
            registerSingleMessageLog(registry, messageLogAttr);
        }

    }

    private void registerMultipleMessageLogReceiverEndpoints(final BeanDefinitionRegistry registry,
                                                             final AnnotationAttributes[] annotationAttributesArr) {
        for (final AnnotationAttributes annotationAttributes : annotationAttributesArr) {
            final String channelName = environment.resolvePlaceholders(annotationAttributes.getString("channelName"));
            final String beanName = Objects.toString(
                    emptyToNull(annotationAttributes.getString("name")),
                    beanNameForMessageLogReceiverEndpoint(channelName));
            final String processorBeanName = beanName + "Processor";
            if (!registry.containsBeanDefinition(beanName)) {
                registerMessageLogReceiverEndpointBeanDefinition(registry, beanName, channelName);
            } else {
                throw new BeanCreationException(beanName, format("MessageLogReceiverEndpoint %s is already registered.", beanName));
            }
            if (!registry.containsBeanDefinition(processorBeanName)) {
                registerMessageLogReceiverEndpointProcessorBeanDefinition(registry, processorBeanName, beanName, channelName);
            } else {
                throw new BeanCreationException(beanName, format("MessageLogReceiverEndpointProcessor %s is already registered.", processorBeanName));
            }

        }
    }

    private void registerSingleMessageLog(final BeanDefinitionRegistry registry,
                                            final MultiValueMap<String, Object> messageLogAttr) {
        if (messageLogAttr != null) {

            final String channelName = environment.resolvePlaceholders(
                    messageLogAttr.getFirst("channelName").toString());

            final String beanName = Objects.toString(
                    emptyToNull(messageLogAttr.getFirst("name").toString()),
                    beanNameForMessageLogReceiverEndpoint(channelName));

            final String processorBeanName = beanName + "Processor";

            if (!registry.containsBeanDefinition(beanName)) {
                registerMessageLogReceiverEndpointBeanDefinition(registry, beanName, channelName);
            } else {
                throw new BeanCreationException(beanName, format("MessageLogReceiverEndpoint %s is already registered.", beanName));
            }
            if (!registry.containsBeanDefinition(processorBeanName)) {
                registerMessageLogReceiverEndpointProcessorBeanDefinition(registry, processorBeanName, beanName, channelName);
            } else {
                throw new BeanCreationException(beanName, format("MessageLogReceiverEndpointProcessor %s is already registered.", processorBeanName));
            }
        }
    }

    private void registerMessageLogReceiverEndpointBeanDefinition(final BeanDefinitionRegistry registry,
                                                                  final String beanName,
                                                                  final String channelName) {


        registry.registerBeanDefinition(
                beanName,
                genericBeanDefinition(DelegateMessageLogReceiverEndpoint.class)
                        .addConstructorArgValue(channelName)
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
