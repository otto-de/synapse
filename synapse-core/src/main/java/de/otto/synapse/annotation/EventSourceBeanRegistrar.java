package de.otto.synapse.annotation;

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

import java.util.Objects;

import static com.google.common.base.Strings.emptyToNull;
import static de.otto.synapse.annotation.BeanNameHelper.beanNameForChannel;
import static java.lang.String.format;
import static org.slf4j.LoggerFactory.getLogger;
import static org.springframework.beans.factory.support.AbstractBeanDefinition.DEPENDENCY_CHECK_ALL;
import static org.springframework.beans.factory.support.BeanDefinitionBuilder.genericBeanDefinition;

/**
 * {@link ImportBeanDefinitionRegistrar} for event-sourcing support.
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
            registerMultipleEventSources(registry, (AnnotationAttributes[]) value);
        } else {
            final MultiValueMap<String, Object> eventSourceAttr = metadata.getAllAnnotationAttributes(EnableEventSource.class.getName(), false);
            registerSingleEventSource(registry, eventSourceAttr);
        }

    }

    private void registerMultipleEventSources(final BeanDefinitionRegistry registry,
                                              final AnnotationAttributes[] annotationAttributesArr) {
        for (final AnnotationAttributes annotationAttributes : annotationAttributesArr) {
            final String channelName = environment.resolvePlaceholders(annotationAttributes.getString("channelName"));
            final String beanName = Objects.toString(
                    emptyToNull(annotationAttributes.getString("name")),
                    beanNameForChannel(channelName));
            if (!registry.containsBeanDefinition(beanName)) {
                registerBeanDefinition(registry, beanName, channelName.isEmpty() ? beanName : channelName);
            } else {
                throw new BeanCreationException(beanName, format("EventSource %s is already registered.", beanName));
            }
        }
    }

    private void registerSingleEventSource(final BeanDefinitionRegistry registry,
                                           final MultiValueMap<String, Object> eventSourceAttr) {
        if (eventSourceAttr != null) {
            final String streamName = environment.resolvePlaceholders(
                    eventSourceAttr.getFirst("channelName").toString());
            final String beanName = Objects.toString(
                    emptyToNull(eventSourceAttr.getFirst("name").toString()),
                    beanNameForChannel(streamName));

            if (!registry.containsBeanDefinition(beanName)) {
                registerBeanDefinition(registry, beanName, streamName.isEmpty() ? beanName : streamName);
            } else {
                throw new BeanCreationException(beanName, format("EventSource %s is already registered.", beanName));
            }
        }
    }

    private void registerBeanDefinition(final BeanDefinitionRegistry registry,
                                        final String beanName,
                                        final String channelName) {
        registry.registerBeanDefinition(
                beanName,
                genericBeanDefinition(DelegateEventSource.class)
                        .addConstructorArgValue(beanName)
                        .addConstructorArgValue(channelName)
                        .setDependencyCheck(DEPENDENCY_CHECK_ALL)
                        .getBeanDefinition()
        );
        LOG.info("Registered EventSource {} with for channelName {}", beanName, channelName);
    }

}
