package de.otto.synapse.annotation;

import de.otto.synapse.endpoint.MessageInterceptorRegistration;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.MethodInvokingMessageInterceptor;
import org.slf4j.Logger;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.Ordered;

import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.collect.ImmutableSet.copyOf;
import static java.util.Collections.newSetFromMap;
import static java.util.regex.Pattern.compile;
import static org.slf4j.LoggerFactory.getLogger;
import static org.springframework.core.MethodIntrospector.selectMethods;
import static org.springframework.core.annotation.AnnotationUtils.findAnnotation;

public class MessageInterceptorBeanPostProcessor implements BeanPostProcessor, Ordered, ApplicationContextAware {

    private static final Logger LOG = getLogger(MessageInterceptorBeanPostProcessor.class);

    private final Set<Class<?>> nonAnnotatedClasses = newSetFromMap(new ConcurrentHashMap<>(64));


    private ConfigurableApplicationContext applicationContext;

    @Override
    public int getOrder() {
        return LOWEST_PRECEDENCE;
    }

    @Override
    public void setApplicationContext(final ApplicationContext applicationContext) throws BeansException {
        if (applicationContext instanceof ConfigurableApplicationContext) {
            this.applicationContext = (ConfigurableApplicationContext) applicationContext;
        }
    }

    @Override
    public Object postProcessBeforeInitialization(final Object bean, final String beanName) {
        if (!this.nonAnnotatedClasses.contains(bean.getClass())) {
            final Class<?> targetClass = AopUtils.getTargetClass(bean);
            final Map<Method, Set<MessageInterceptor>> annotatedMethods = findMethodsAnnotatedWithMessageInterceptor(targetClass);
            if (annotatedMethods.isEmpty()) {
                this.nonAnnotatedClasses.add(bean.getClass());
                LOG.trace("No @MessageInterceptor annotations found on bean type: {}", bean.getClass());
            } else {
                registerMessageInterceptors(bean, beanName, annotatedMethods);
            }
        }
        return bean;
    }

    @Override
    public Object postProcessAfterInitialization(final Object bean, final String beanName) {
        return bean;
    }

    private Map<Method, Set<MessageInterceptor>> findMethodsAnnotatedWithMessageInterceptor(Class<?> targetClass) {
        return selectMethods(targetClass,
                (MethodIntrospector.MetadataLookup<Set<MessageInterceptor>>) method -> {
                    final Set<MessageInterceptor> consumerAnnotations = messageInterceptorAnnotationsOf(method);
                    return (!consumerAnnotations.isEmpty() ? consumerAnnotations : null);
                });
    }

    private void registerMessageInterceptors(final Object bean,
                                             final String beanName,
                                             final Map<Method, Set<MessageInterceptor>> annotatedMethods) {
        for (Map.Entry<Method, Set<MessageInterceptor>> entry : annotatedMethods.entrySet()) {
            final Method method = entry.getKey();
            final MessageInterceptorRegistry registry = applicationContext.getBean(MessageInterceptorRegistry.class);
            for (final MessageInterceptor interceptorAnnocation : entry.getValue()) {
                registry.register(
                        registrationFor(interceptorAnnocation, messageInterceptorFor(bean, method))
                );
            }
        }
        LOG.info("{} @EventSourceConsumer methods processed on bean {} : {}'", annotatedMethods.size(), beanName, annotatedMethods);
    }

    private MessageInterceptorRegistration registrationFor(MessageInterceptor interceptorAnnotation, de.otto.synapse.endpoint.MessageInterceptor interceptor) {
        return new MessageInterceptorRegistration(
                compile(interceptorAnnotation.channelNamePattern()),
                interceptor,
                copyOf(interceptorAnnotation.endpointType()));
    }

    /*
     * AnnotationUtils.getRepeatableAnnotations does not look at interfaces
     */
    private Set<MessageInterceptor> messageInterceptorAnnotationsOf(final Method method) {
        final Set<MessageInterceptor> interceptorAnnotations = new HashSet<>();
        MessageInterceptor ann = findAnnotation(method, MessageInterceptor.class);
        if (ann != null) {
            interceptorAnnotations.add(ann);
        }
        return interceptorAnnotations;
    }

    private MethodInvokingMessageInterceptor messageInterceptorFor(final Object bean,
                                                                   final Method annotatedMethod) {
        return new MethodInvokingMessageInterceptor(bean, annotatedMethod);
    }

}
