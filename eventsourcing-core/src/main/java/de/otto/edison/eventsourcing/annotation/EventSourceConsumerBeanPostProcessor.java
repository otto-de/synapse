package de.otto.edison.eventsourcing.annotation;

import de.otto.edison.eventsourcing.consumer.EventSource;
import de.otto.edison.eventsourcing.consumer.MethodInvokingMessageConsumer;
import org.slf4j.Logger;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.AnnotationUtils;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.slf4j.LoggerFactory.getLogger;
import static org.springframework.core.MethodIntrospector.selectMethods;

public class EventSourceConsumerBeanPostProcessor implements BeanPostProcessor, Ordered, ApplicationContextAware {

    private static final Logger LOG = getLogger(EventSourceConsumerBeanPostProcessor.class);

    private final Set<Class<?>> nonAnnotatedClasses =
            Collections.newSetFromMap(new ConcurrentHashMap<Class<?>, Boolean>(64));


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
            final Map<Method, Set<EventSourceConsumer>> annotatedMethods = findMethodsAnnotatedWithEventSourceConsumer(targetClass);
            if (annotatedMethods.isEmpty()) {
                this.nonAnnotatedClasses.add(bean.getClass());
                LOG.trace("No @EventSourceConsumer annotations found on bean type: {}", bean.getClass());
            } else {
                registerEventConsumers(bean, beanName, annotatedMethods);
            }
        }
        return bean;
    }

    @Override
    public Object postProcessAfterInitialization(final Object bean, final String beanName) {
        return bean;
    }

    private Map<Method, Set<EventSourceConsumer>> findMethodsAnnotatedWithEventSourceConsumer(Class<?> targetClass) {
        return selectMethods(targetClass,
                (MethodIntrospector.MetadataLookup<Set<EventSourceConsumer>>) method -> {
                    final Set<EventSourceConsumer> consumerAnnotations = consumerAnnotationsOf(method);
                    return (!consumerAnnotations.isEmpty() ? consumerAnnotations : null);
                });
    }

    private void registerEventConsumers(final Object bean,
                                        final String beanName,
                                        final Map<Method, Set<EventSourceConsumer>> annotatedMethods) {
        for (Map.Entry<Method, Set<EventSourceConsumer>> entry : annotatedMethods.entrySet()) {
            final Method method = entry.getKey();
            for (EventSourceConsumer consumerAnnotation : entry.getValue()) {
                matchingEventSourceFor(consumerAnnotation)
                        .register(eventConsumerFor(consumerAnnotation, method, bean));
            }
        }
        LOG.info("{} @EventSourceConsumer methods processed on bean {} : {}'", annotatedMethods.size(), beanName, annotatedMethods);
    }

    /*
     * AnnotationUtils.getRepeatableAnnotations does not look at interfaces
     */
    private Set<EventSourceConsumer> consumerAnnotationsOf(final Method method) {
        Set<EventSourceConsumer> listeners = new HashSet<>();
        EventSourceConsumer ann = AnnotationUtils.findAnnotation(method, EventSourceConsumer.class);
        if (ann != null) {
            listeners.add(ann);
        }
        return listeners;
    }

    private MethodInvokingMessageConsumer<?> eventConsumerFor(final EventSourceConsumer annotation,
                                                              final Method annotatedMethod,
                                                              final Object bean) {
        return new MethodInvokingMessageConsumer<>(annotation.keyPattern(), annotation.payloadType(), bean, annotatedMethod);
    }

    private EventSource matchingEventSourceFor(final EventSourceConsumer annotation) {
        return applicationContext.getBean(annotation.eventSource(), EventSource.class);
    }

}
