package de.otto.edison.eventsourcing.annotation;

import de.otto.edison.eventsourcing.consumer.MethodInvokingEventConsumer;
import org.slf4j.Logger;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
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

public class EventSourceConsumerAnnotationBeanPostProcessor implements BeanPostProcessor, Ordered, ApplicationContextAware {

    private static final Logger LOG = getLogger(EventSourceConsumerAnnotationBeanPostProcessor.class);

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
    public Object postProcessBeforeInitialization(final Object bean, final String beanName) throws BeansException {
        return bean;
    }

    @Override
    public Object postProcessAfterInitialization(final Object bean, final String beanName) throws BeansException {
        if (!this.nonAnnotatedClasses.contains(bean.getClass())) {
            final Class<?> targetClass = AopUtils.getTargetClass(bean);
            final Map<Method, Set<EventSourceConsumer>> annotatedMethods = MethodIntrospector.selectMethods(targetClass,
                    (MethodIntrospector.MetadataLookup<Set<EventSourceConsumer>>) method -> {
                        Set<EventSourceConsumer> listenerMethods = findListenerAnnotations(method);
                        return (!listenerMethods.isEmpty() ? listenerMethods : null);
                    });
            if (annotatedMethods.isEmpty()) {
                this.nonAnnotatedClasses.add(bean.getClass());
                if (LOG.isTraceEnabled()) {
                    LOG.trace("No @EventSourceConsumer annotations found on bean type: " + bean.getClass());
                }
            }
            else {
                // Non-empty set of methods
                for (Map.Entry<Method, Set<EventSourceConsumer>> entry : annotatedMethods.entrySet()) {
                    final Method method = entry.getKey();
                    for (EventSourceConsumer consumer : entry.getValue()) {
                        registerEventConsumer(consumer, method, bean, beanName);
                    }
                }
                if (LOG.isInfoEnabled()) {
                    LOG.info(annotatedMethods.size() + " @EventSourceConsumer methods processed on bean '"
                            + beanName + "': " + annotatedMethods);
                }
            }
        }
        return bean;
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

    private void registerEventConsumer(final EventSourceConsumer annotation,
                                       final Method annotatedMethod,
                                       final Object bean,
                                       final String beanName) {
        final String streamName = applicationContext.getEnvironment().resolvePlaceholders(annotation.streamName());
        final MethodInvokingEventConsumer eventConsumer = new MethodInvokingEventConsumer(streamName, bean, annotatedMethod);
        final ConfigurableListableBeanFactory beanFactory = applicationContext.getBeanFactory();
        if (!beanFactory.containsBean(annotation.name())) {
            this.applicationContext.getBeanFactory().registerSingleton(annotation.name(), eventConsumer);
        }
    }

}