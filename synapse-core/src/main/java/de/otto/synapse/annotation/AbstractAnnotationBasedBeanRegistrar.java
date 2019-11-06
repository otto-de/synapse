package de.otto.synapse.annotation;

import org.slf4j.Logger;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotationMetadata;

import java.lang.annotation.Annotation;
import java.lang.annotation.Repeatable;
import java.util.Objects;

import static com.google.common.base.Strings.emptyToNull;
import static de.otto.synapse.annotation.BeanNameHelper.beanNameFor;
import static java.util.Objects.requireNonNull;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * {@link ImportBeanDefinitionRegistrar} for message log support.
 *
 * @see EnableMessageLogReceiverEndpoint
 */
public abstract class AbstractAnnotationBasedBeanRegistrar implements ImportBeanDefinitionRegistrar, EnvironmentAware {

    private static final Logger LOG = getLogger(AbstractAnnotationBasedBeanRegistrar.class);

    private Environment environment;

    /**
     * Set the {@code Environment} that this component runs in.
     *
     * @param environment the current Spring environment
     */
    @Override
    public final  void setEnvironment(final Environment environment) {
        this.environment = environment;
    }

    public final Environment getEnvironment() {
        return environment;
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
        final Class<? extends Annotation> annotationType = getAnnotationType();
        final Class<? extends Annotation> repeatableAnnotationType = annotationType.isAnnotationPresent(Repeatable.class)
                ? annotationType.getAnnotation(Repeatable.class).value()
                : null;
        if (repeatableAnnotationType != null && metadata.hasAnnotation(repeatableAnnotationType.getName())) {
            final AnnotationAttributes annotationAttributes = (AnnotationAttributes) metadata.getAnnotationAttributes(repeatableAnnotationType.getName(), false);
            final AnnotationAttributes[] value = requireNonNull(annotationAttributes).getAnnotationArray("value");
            for (AnnotationAttributes attributes : value) {
                registerBeanDefinitions(attributes, registry);
            }
        } else if (metadata.hasAnnotation(annotationType.getName())) {
            final AnnotationAttributes annotationAttributes = (AnnotationAttributes) metadata.getAnnotationAttributes(annotationType.getName(), false);
            registerBeanDefinitions(annotationAttributes, registry);
        }

    }

    protected abstract Class<? extends Annotation> getAnnotationType();

    protected abstract void registerBeanDefinitions(final String channelName,
                                                    final String beanName,
                                                    final AnnotationAttributes annotationAttributes,
                                                    final BeanDefinitionRegistry registry);

    private void registerBeanDefinitions(final AnnotationAttributes annotationAttributes,
                                         final BeanDefinitionRegistry registry) {
        if (annotationAttributes != null) {

            final String channelName = getChannelName(annotationAttributes);
            final String beanName = getBeanName(annotationAttributes, channelName);
            registerBeanDefinitions(channelName, beanName, annotationAttributes, registry);
        }
    }

    private String getBeanName(final AnnotationAttributes annotationAttributes,
                               final String channelName) {
        return Objects.toString(
                        emptyToNull(annotationAttributes.getString("name")),
                        beanNameFor(getAnnotationType(), channelName));
    }

    private String getChannelName(final AnnotationAttributes annotationAttributes) {
        return environment.resolvePlaceholders(
                        annotationAttributes.getString("channelName"));
    }

}
