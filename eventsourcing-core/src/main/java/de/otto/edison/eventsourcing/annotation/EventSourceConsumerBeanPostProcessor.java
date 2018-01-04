package de.otto.edison.eventsourcing.annotation;

import com.google.common.base.CaseFormat;
import de.otto.edison.eventsourcing.EventSourceFactory;
import de.otto.edison.eventsourcing.consumer.EventConsumer;
import de.otto.edison.eventsourcing.consumer.EventSource;
import de.otto.edison.eventsourcing.consumer.MethodInvokingEventConsumer;
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
        return bean;
    }

    @Override
    public Object postProcessAfterInitialization(final Object bean, final String beanName) {
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

    private Map<Method, Set<EventSourceConsumer>> findMethodsAnnotatedWithEventSourceConsumer(Class<?> targetClass) {
        return MethodIntrospector.selectMethods(targetClass,
                (MethodIntrospector.MetadataLookup<Set<EventSourceConsumer>>) method -> {
                    Set<EventSourceConsumer> listenerMethods = findListenerAnnotations(method);
                    return (!listenerMethods.isEmpty() ? listenerMethods : null);
                });
    }

    private void registerEventConsumers(final Object bean,
                                        final String beanName,
                                        final Map<Method, Set<EventSourceConsumer>> annotatedMethods) {
        for (Map.Entry<Method, Set<EventSourceConsumer>> entry : annotatedMethods.entrySet()) {
            final Method method = entry.getKey();
            for (EventSourceConsumer consumerAnnotation : entry.getValue()) {
                EventConsumer<?> eventConsumer = registerEventConsumer(consumerAnnotation, method, bean);
                EventSource eventSource = registerEventSource(consumerAnnotation);
                eventSource.register(eventConsumer);
            }
        }
        LOG.info("{} @EventSourceConsumer methods processed on bean {} : {}'", annotatedMethods.size(), beanName, annotatedMethods);
    }

    /*
     * AnnotationUtils.getRepeatableAnnotations does not look at interfaces
     */
    private Set<EventSourceConsumer> findListenerAnnotations(final Method method) {
        Set<EventSourceConsumer> listeners = new HashSet<>();
        EventSourceConsumer ann = AnnotationUtils.findAnnotation(method, EventSourceConsumer.class);
        if (ann != null) {
            listeners.add(ann);
        }
        return listeners;
    }

    private boolean eventSourceExists(String streamName) {
        return applicationContext.containsBean(streamNameToEventSourceName(streamName));
    }

    private String streamNameToEventSourceName(String streamName) {
        return CaseFormat.LOWER_HYPHEN.to(CaseFormat.LOWER_CAMEL, streamName) + "EventSource";
    }

    private MethodInvokingEventConsumer<?> registerEventConsumer(final EventSourceConsumer annotation,
                                                              final Method annotatedMethod,
                                                              final Object bean) {
        final String streamName = applicationContext.getEnvironment().resolvePlaceholders(annotation.streamName());
        final MethodInvokingEventConsumer<?> eventConsumer = new MethodInvokingEventConsumer<>(streamName, annotation.keyPattern(), annotation.payloadType(), bean, annotatedMethod);
        applicationContext.getBeanFactory().registerSingleton(annotation.name(), eventConsumer);
        return eventConsumer;
    }

    @SuppressWarnings("unchecked")
    private EventSource registerEventSource(final EventSourceConsumer annotation) {
        Class<? extends EventSource> eventSourceType = annotation.eventSourceType();

        String resolvedStreamName = applicationContext.getEnvironment().resolvePlaceholders(annotation.streamName());
        if (eventSourceExists(resolvedStreamName)) {
            return applicationContext.getBean(streamNameToEventSourceName(resolvedStreamName), EventSource.class);
        }

        EventSourceFactory eventSourceFactory = applicationContext.getBean(EventSourceFactory.class);
        EventSource eventSource = eventSourceFactory.createEventSource(eventSourceType, resolvedStreamName);
        applicationContext.getBeanFactory().registerSingleton(streamNameToEventSourceName(resolvedStreamName), eventSource);
        return eventSource;
    }

}