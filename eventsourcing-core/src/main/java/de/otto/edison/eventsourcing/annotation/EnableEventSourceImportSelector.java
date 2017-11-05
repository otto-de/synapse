package de.otto.edison.eventsourcing.annotation;

import de.otto.edison.eventsourcing.CompactingKinesisEventSource;
import org.slf4j.Logger;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.context.annotation.ImportSelector;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.util.MultiValueMap;

import static org.slf4j.LoggerFactory.getLogger;
import static org.springframework.beans.factory.support.AbstractBeanDefinition.AUTOWIRE_BY_NAME;
import static org.springframework.beans.factory.support.AbstractBeanDefinition.DEPENDENCY_CHECK_ALL;
import static org.springframework.beans.factory.support.BeanDefinitionBuilder.genericBeanDefinition;

public class EnableEventSourceImportSelector implements ImportSelector {

    private static final Logger LOG = getLogger(EnableEventSourceImportSelector.class);

    @Override
    public String[] selectImports(AnnotationMetadata metadata) {
        return new String[] {
                EventSourceBeanRegistrar.class.getName()
        };
    }

    /**
     * {@link ImportBeanDefinitionRegistrar} for event-sourcing support.
     */
    public static class EventSourceBeanRegistrar implements ImportBeanDefinitionRegistrar, EnvironmentAware {

        private Environment environment;

        /**
         * Set the {@code Environment} that this component runs in.
         *
         * @param environment
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
            final MultiValueMap<String, Object> eventSourcesAttr = metadata
                    .getAllAnnotationAttributes(EnableEventSources.class.getName(), false);
            if (eventSourcesAttr != null) {
                final Object value = eventSourcesAttr.getFirst("value");
                registerMultipleEventSources(registry, (AnnotationAttributes[]) value);
            } else {
                final MultiValueMap<String, Object> eventSourceAttr = metadata
                        .getAllAnnotationAttributes(EnableEventSource.class.getName(), false);
                registerSingleEventSource(registry, eventSourceAttr);
            }

        }

        private void registerMultipleEventSources(final BeanDefinitionRegistry registry,
                                                  final AnnotationAttributes[] annotationAttributesArr) {
            for (final AnnotationAttributes annotationAttributes : annotationAttributesArr) {
                final String beanName = annotationAttributes.getString("name");
                final String streamName = environment.resolvePlaceholders(
                        annotationAttributes.getString("streamName"));
                final Class<?> payloadType = annotationAttributes.getClass("payloadType");
                if (!registry.containsBeanDefinition(beanName)) {
                    registerBeanDefinition(registry, beanName, streamName.isEmpty() ? beanName : streamName, payloadType);
                }
            }
        }

        private void registerSingleEventSource(final BeanDefinitionRegistry registry,
                                               final MultiValueMap<String, Object> eventSourceAttr) {
            if (eventSourceAttr != null) {
                final String beanName = eventSourceAttr.getFirst("name").toString();
                final String streamName = environment.resolvePlaceholders(
                        eventSourceAttr.getFirst("streamName").toString());
                final Class<?> payloadType = asClass(eventSourceAttr.getFirst("payloadType"));

                if (!registry.containsBeanDefinition(beanName)) {
                    registerBeanDefinition(registry, beanName, streamName.isEmpty() ? beanName: streamName, payloadType);
                }
            }
        }

        private Class<?> asClass(final Object payloadType) {
            if (payloadType instanceof Class && payloadType != void.class) {
                return (Class<?>) payloadType;
            } else {
                return null;
            }
        }

        private void registerBeanDefinition(final BeanDefinitionRegistry registry,
                                            final String beanName,
                                            final String streamName,
                                            final Class<?> payloadType) {
            registry.registerBeanDefinition(
                    beanName,
                    genericBeanDefinition(CompactingKinesisEventSource.class)
                            .addConstructorArgValue(streamName)
                            .addConstructorArgValue(payloadType)
                            .setDependencyCheck(DEPENDENCY_CHECK_ALL)
                            .setAutowireMode(AUTOWIRE_BY_NAME)
                            .getBeanDefinition()
            );
            LOG.info("Registered CompactingKinesisEventSource with beanName {} for streamName {}", beanName, streamName);
        }

    }

}