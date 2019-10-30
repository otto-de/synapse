package de.otto.synapse.annotation;

import de.otto.synapse.endpoint.receiver.DelegateMessageLogReceiverEndpoint;
import de.otto.synapse.eventsource.DelegateEventSource;
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
import static de.otto.synapse.annotation.BeanNameHelper.beanNameForEventSource;
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
public class EventSourceBeanRegistrar implements ImportBeanDefinitionRegistrar, EnvironmentAware {

    private static final Logger LOG = getLogger(EventSourceBeanRegistrar.class);

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
        @EnableEventSource is a @Repeatable annotation. If there are multiple annotations present,
        there is an automagically added @EnableEventsources annotation, containing the @EnableEventSource
        annotations as value.
         */
        final MultiValueMap<String, Object> eventSourcesAttr = metadata.getAllAnnotationAttributes(EnableEventSources.class.getName(), false);
        if (eventSourcesAttr != null) {
            final Object value = eventSourcesAttr.getFirst("value");
            if (value == null) {
                return;
            }
            LinkedHashMap[] castedValue = (LinkedHashMap[])value;
            AnnotationAttributes[] attributes = new AnnotationAttributes[castedValue.length];
            for(int i=0; i<castedValue.length; i++) {
                attributes[i] = new AnnotationAttributes(castedValue[i]);
            }
            registerMultipleEventSources(registry, attributes);
        } else {
            final MultiValueMap<String, Object> eventSourceAttr = metadata.getAllAnnotationAttributes(EnableEventSource.class.getName(), false);
            registerSingleEventSource(registry, eventSourceAttr);
        }

    }

    private void registerMultipleEventSources(final BeanDefinitionRegistry registry,
                                              final AnnotationAttributes[] annotationAttributesArr) {
        for (final AnnotationAttributes annotationAttributes : annotationAttributesArr) {
            final String channelName = environment.resolvePlaceholders(annotationAttributes.getString("channelName"));
            final String eventSourceBeanName = Objects.toString(
                    emptyToNull(annotationAttributes.getString("name")),
                    beanNameForEventSource(channelName));
            final String messageLogBeanName = Objects.toString(
                    emptyToNull(annotationAttributes.getString("messageLogReceiverEndpoint")),
                    beanNameForMessageLogReceiverEndpoint(channelName));
            registerBeans(registry, channelName, eventSourceBeanName, messageLogBeanName);
        }
    }

    private void registerSingleEventSource(final BeanDefinitionRegistry registry,
                                           final MultiValueMap<String, Object> eventSourceAttr) {
        if (eventSourceAttr != null) {

            final String channelName = environment.resolvePlaceholders(
                    eventSourceAttr.getFirst("channelName").toString());

            final String eventSourceBeanName = Objects.toString(
                    emptyToNull(eventSourceAttr.getFirst("name").toString()),
                    beanNameForEventSource(channelName));

            final String messageLogBeanName = Objects.toString(
                    emptyToNull(eventSourceAttr.getFirst("messageLogReceiverEndpoint").toString()),
                    beanNameForMessageLogReceiverEndpoint(channelName));

            registerBeans(registry, channelName, eventSourceBeanName, messageLogBeanName);
        }
    }

    private void registerBeans(BeanDefinitionRegistry registry, String channelName, String eventSourceBeanName, String messageLogBeanName) {
        if (!registry.containsBeanDefinition(messageLogBeanName)) {
            registerMessageLogBeanDefinition(registry, messageLogBeanName, channelName);
        } else {
            throw new BeanCreationException(messageLogBeanName, format("MessageLogReceiverEndpoint %s is already registered.", messageLogBeanName));
        }
        if (!registry.containsBeanDefinition(eventSourceBeanName)) {
            registerEventSourceBeanDefinition(registry, eventSourceBeanName, messageLogBeanName, channelName);
        } else {
            throw new BeanCreationException(eventSourceBeanName, format("EventSource %s is already registered.", eventSourceBeanName));
        }
    }

    private void registerMessageLogBeanDefinition(final BeanDefinitionRegistry registry,
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

    private void registerEventSourceBeanDefinition(final BeanDefinitionRegistry registry,
                                                   final String beanName,
                                                   final String messageLogBeanName,
                                                   final String channelName) {
        registry.registerBeanDefinition(
                beanName,
                genericBeanDefinition(DelegateEventSource.class)
                        .addConstructorArgValue(messageLogBeanName)
                        .setDependencyCheck(DEPENDENCY_CHECK_ALL)
                        .getBeanDefinition()
        );
        LOG.info("Registered EventSource {} with for channelName {}", beanName, channelName);
    }

}
