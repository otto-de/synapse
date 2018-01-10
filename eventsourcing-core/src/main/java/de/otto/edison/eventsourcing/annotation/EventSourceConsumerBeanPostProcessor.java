package de.otto.edison.eventsourcing.annotation;

import de.otto.edison.eventsourcing.consumer.EventConsumer;
import de.otto.edison.eventsourcing.consumer.EventSource;
import de.otto.edison.eventsourcing.consumer.MethodInvokingEventConsumer;
import org.slf4j.Logger;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.AnnotationUtils;

import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static de.otto.edison.eventsourcing.annotation.BeanNameHelper.beanNameForStream;
import static java.lang.String.format;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toList;
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
                EventSource eventSource = matchingEventSourceFor(beanName, method, consumerAnnotation);
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

    private MethodInvokingEventConsumer<?> registerEventConsumer(final EventSourceConsumer annotation,
                                                                 final Method annotatedMethod,
                                                                 final Object bean) {
        final String streamName = applicationContext.getEnvironment().resolvePlaceholders(annotation.streamName());
        final MethodInvokingEventConsumer<?> eventConsumer = new MethodInvokingEventConsumer<>(streamName, annotation.keyPattern(), annotation.payloadType(), bean, annotatedMethod);
//        applicationContext.getBeanFactory().registerSingleton(annotation.name(), eventConsumer);
        return eventConsumer;
    }

    @SuppressWarnings("unchecked")
    private EventSource matchingEventSourceFor(final String beanName, final Method method, final EventSourceConsumer annotation) {
        final String resolvedStreamName = applicationContext.getEnvironment().resolvePlaceholders(annotation.streamName());
        final String eventSourceName = beanNameForStream(resolvedStreamName);
        if (applicationContext.containsBean(eventSourceName)) {
            try {
                return applicationContext.getBean(eventSourceName, EventSource.class);
            } catch (final NoSuchBeanDefinitionException e) {
                final String[] availableEventSources = applicationContext.getBeanNamesForType(EventSource.class);
                if (availableEventSources.length > 0) {
                    throw new NoSuchBeanDefinitionException(
                            e.getBeanName(),
                            format("The @EventSourceConsumer '%s#%s' for stream-name '%s' can not be registered because there is no matching EventSource. Available EventSources are '%s'", beanName, method.getName(), resolvedStreamName, Arrays.toString(availableEventSources)));
                } else {
                    throw new NoSuchBeanDefinitionException(
                            e.getBeanName(),
                            format("The @EventSourceConsumer '%s#%s' for stream-name '%s' can not be registered because there is no matching EventSource. You need to configure the EventSource manually as a Spring Bean, or by using @EnableEventSource.", beanName, method.getName(), resolvedStreamName));
                }
            }
        } else {
            final String[] eventSources = applicationContext.getBeanNamesForType(EventSource.class);
            final List<EventSource> sources = stream(eventSources)
                    .filter(name -> annotation.eventSource().isEmpty() || annotation.eventSource().equals(name))
                    .map(name -> applicationContext.getBean(name, EventSource.class))
                    .filter(es -> es.getStreamName().equals(resolvedStreamName))
                    .collect(toList());
            if (sources.size() == 0) {
                final String[] availableEventSources = applicationContext.getBeanNamesForType(EventSource.class);
                throw new NoSuchBeanDefinitionException(
                        eventSourceName,
                        format("The @EventSourceConsumer '%s#%s' for stream-name '%s' can not be registered because there is no matching EventSource. You need to configure the EventSource manually as a Spring Bean, or by using @EnableEventSource.", beanName, method.getName(), resolvedStreamName));
            } else if (sources.size() == 1) {
                return sources.get(0);
            } else {
                throw new IllegalStateException(format("The @EventSourceConsumer '%s#%s' for stream-name '%s' can not be registered because there are multiple EventSource beans for the specified stream available: '%s'", beanName, method.getName(), resolvedStreamName, sources));
            }
        }

    }

}