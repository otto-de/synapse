package de.otto.synapse.journal;

import com.google.common.collect.ImmutableSet;
import de.otto.synapse.annotation.EventSourceConsumer;
import de.otto.synapse.endpoint.EndpointType;
import de.otto.synapse.endpoint.MessageInterceptorRegistration;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.eventsource.EventSource;
import de.otto.synapse.messagestore.MessageStore;
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

import static java.util.Collections.newSetFromMap;
import static java.util.regex.Pattern.compile;
import static org.slf4j.LoggerFactory.getLogger;
import static org.springframework.core.MethodIntrospector.selectMethods;
import static org.springframework.core.annotation.AnnotationUtils.findAnnotation;

/**
 * A BeanPostProcessor that is looking for {@link JournalingStateRepository} beans, finds methods
 * annotated as {@link EventSourceConsumer} in the state repository bean, and registers
 * {@link JournalingInterceptor journaling interceptors} for every annotated method, so that messages
 * that are updating the entites stored in the {@code JournalingStateRepository} will be added to the
 * {@link MessageStore} associated with the state repository.
 *
 */
public class JournalingStateRepositoryBeanPostProcessor implements BeanPostProcessor, Ordered, ApplicationContextAware {

    private static final Logger LOG = getLogger(JournalingStateRepositoryBeanPostProcessor.class);

    private final Set<Class<?>> nonAnnotatedClasses = newSetFromMap(new ConcurrentHashMap<>(64));

    private ConfigurableApplicationContext applicationContext;

    public JournalingStateRepositoryBeanPostProcessor() {
    }

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
        final Journals journals = applicationContext.getBean(Journals.class);
        if (!this.nonAnnotatedClasses.contains(bean.getClass())) {
            final Class<?> targetClass = AopUtils.getTargetClass(bean);
            if (Journal.class.isAssignableFrom(targetClass)) {
                journals.add((Journal) bean);
                LOG.debug("Found JournalingStateRepository bean {} of type {}: Searching for @EventSourceConsumer methods.", beanName, bean.getClass().getSimpleName());
                final Map<Method, Set<EventSourceConsumer>> annotatedMethods = findMethodsAnnotatedWithEventSourceConsumer(targetClass);
                if (annotatedMethods.isEmpty()) {
                    this.nonAnnotatedClasses.add(bean.getClass());
                    LOG.warn("No @EventSourceConsumer methods found on bean type: {}. Messages must be manually added to the state repository's MessageStore", bean.getClass().getSimpleName());
                } else {
                    registerMessageInterceptors((Journal) bean, annotatedMethods);
                }
            } else {
                this.nonAnnotatedClasses.add(bean.getClass());
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
                    final Set<EventSourceConsumer> consumerAnnotations = eventSourceConsumerAnnotationsOf(method);
                    return (!consumerAnnotations.isEmpty() ? consumerAnnotations : null);
                });
    }

    private void registerMessageInterceptors(final Journal journal,
                                             final Map<Method, Set<EventSourceConsumer>> annotatedMethods) {
        for (Map.Entry<Method, Set<EventSourceConsumer>> entry : annotatedMethods.entrySet()) {
            final Method method = entry.getKey();
            final MessageInterceptorRegistry registry = applicationContext.getBean(MessageInterceptorRegistry.class);
            for (final EventSourceConsumer consumerAnnotation : entry.getValue()) {
                registry.register(
                        registrationFor(consumerAnnotation, journal)
                );
                LOG.info("Registered Journal for method {}", method.getName());
            }
        }
    }

    private MessageInterceptorRegistration registrationFor(final EventSourceConsumer consumerAnnotation,
                                                           final Journal journal) {
        final EventSource eventSource = this.applicationContext.getBean(consumerAnnotation.eventSource(), EventSource.class);
        final JournalingInterceptor interceptor = new JournalingInterceptor(
                eventSource.getChannelName(),
                journal);
        return new MessageInterceptorRegistration(
                    compile(eventSource.getChannelName()),
                    interceptor,
                    ImmutableSet.of(EndpointType.RECEIVER),
                    LOWEST_PRECEDENCE);
    }

    /*
     * AnnotationUtils.getRepeatableAnnotations does not look at interfaces
     */
    private Set<EventSourceConsumer> eventSourceConsumerAnnotationsOf(final Method method) {
        final Set<EventSourceConsumer> interceptorAnnotations = new HashSet<>();
        EventSourceConsumer ann = findAnnotation(method, EventSourceConsumer.class);
        if (ann != null) {
            interceptorAnnotations.add(ann);
        }
        return interceptorAnnotations;
    }

}
