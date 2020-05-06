package de.otto.synapse.annotation;

import de.otto.synapse.channel.selector.MessageLog;
import de.otto.synapse.endpoint.receiver.DelegateMessageLogReceiverEndpoint;
import de.otto.synapse.eventsource.DelegateEventSource;
import org.slf4j.Logger;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.annotation.AnnotationAttributes;

import java.lang.annotation.Annotation;
import java.util.Objects;

import static com.google.common.base.Strings.emptyToNull;
import static de.otto.synapse.annotation.BeanNameHelper.beanNameForMessageLogReceiverEndpoint;
import static java.lang.String.format;
import static org.slf4j.LoggerFactory.getLogger;
import static org.springframework.beans.factory.support.AbstractBeanDefinition.DEPENDENCY_CHECK_ALL;
import static org.springframework.beans.factory.support.BeanDefinitionBuilder.genericBeanDefinition;

/**
 * {@link ImportBeanDefinitionRegistrar} for event-sourcing support.
 *
 * @see EnableEventSource
 */
public class EventSourceBeanRegistrar extends AbstractAnnotationBasedBeanRegistrar {

    private static final Logger LOG = getLogger(EventSourceBeanRegistrar.class);

    @Override
    protected Class<? extends Annotation> getAnnotationType() {
        return EnableEventSource.class;
    }

    @Override
    protected void registerBeanDefinitions(final String channelName,
                                           final String beanName,
                                           final AnnotationAttributes annotationAttributes,
                                           final BeanDefinitionRegistry registry) {
        final Class<? extends MessageLog> channelSelector = annotationAttributes.getClass("selector");
        final String messageLogBeanName = Objects.toString(
                emptyToNull(annotationAttributes.getString("messageLogReceiverEndpoint")),
                beanNameForMessageLogReceiverEndpoint(channelName));

        if (!registry.containsBeanDefinition(messageLogBeanName)) {
            registerMessageLogBeanDefinition(registry, messageLogBeanName, channelName, channelSelector);
        } else {
            throw new BeanCreationException(messageLogBeanName, format("MessageLogReceiverEndpoint %s is already registered.", messageLogBeanName));
        }
        if (!registry.containsBeanDefinition(beanName)) {
            registerEventSourceBeanDefinition(registry, beanName, messageLogBeanName, channelName, channelSelector);
        } else {
            throw new BeanCreationException(beanName, format("EventSource %s is already registered.", beanName));
        }
    }

    private void registerMessageLogBeanDefinition(final BeanDefinitionRegistry registry,
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

    private void registerEventSourceBeanDefinition(final BeanDefinitionRegistry registry,
                                                   final String beanName,
                                                   final String messageLogBeanName,
                                                   final String channelName,
                                                   final Class<? extends MessageLog> channelSelector) {
        registry.registerBeanDefinition(
                beanName,
                genericBeanDefinition(DelegateEventSource.class)
                        .addConstructorArgValue(messageLogBeanName)
                        .addConstructorArgValue(channelSelector)
                        .setDependencyCheck(DEPENDENCY_CHECK_ALL)
                        .getBeanDefinition()
        );
        LOG.info("Registered EventSource {} with for channelName {}", beanName, channelName);
    }

}
