package de.otto.synapse.annotation;

import de.otto.synapse.consumer.MethodInvokingMessageConsumer;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpoint;
import de.otto.synapse.endpoint.receiver.MessageReceiverEndpoint;
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

public class MessageLogConsumerBeanPostProcessor implements BeanPostProcessor, Ordered, ApplicationContextAware {

    private static final Logger LOG = getLogger(MessageLogConsumerBeanPostProcessor.class);

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
            final Map<Method, Set<MessageLogConsumer>> annotatedMethods = findMethodsAnnotatedWithMessageLogConsumer(targetClass);
            if (annotatedMethods.isEmpty()) {
                this.nonAnnotatedClasses.add(bean.getClass());
                LOG.trace("No @MessageLogConsumer annotations found on bean type: {}", bean.getClass());
            } else {
                registerMessageLogConsumers(bean, beanName, annotatedMethods);
            }
        }
        return bean;
    }

    private Map<Method, Set<MessageLogConsumer>> findMethodsAnnotatedWithMessageLogConsumer(Class<?> targetClass) {
        return selectMethods(targetClass,
                (MethodIntrospector.MetadataLookup<Set<MessageLogConsumer>>) method -> {
                    final Set<MessageLogConsumer> consumerAnnotations = consumerAnnotationsOf(method);
                    return (!consumerAnnotations.isEmpty() ? consumerAnnotations : null);
                });
    }

    private void registerMessageLogConsumers(final Object bean,
                                               final String beanName,
                                               final Map<Method, Set<MessageLogConsumer>> annotatedMethods) {
        for (Map.Entry<Method, Set<MessageLogConsumer>> entry : annotatedMethods.entrySet()) {
            final Method method = entry.getKey();
            for (MessageLogConsumer consumerAnnotation : entry.getValue()) {
                matchingMessageLogReceiverEndpointFor(consumerAnnotation)
                        .register(MessageLogConsumerFor(consumerAnnotation, method, bean));
            }
        }
        LOG.info("{} @MessageLogConsumer methods processed on bean {} : {}'", annotatedMethods.size(), beanName, annotatedMethods);
    }

    /*
     * AnnotationUtils.getRepeatableAnnotations does not look at interfaces
     */
    private Set<MessageLogConsumer> consumerAnnotationsOf(final Method method) {
        Set<MessageLogConsumer> listeners = new HashSet<>();
        MessageLogConsumer ann = AnnotationUtils.findAnnotation(method, MessageLogConsumer.class);
        if (ann != null) {
            listeners.add(ann);
        }
        return listeners;
    }

    private MethodInvokingMessageConsumer<?> MessageLogConsumerFor(final MessageLogConsumer annotation,
                                                                   final Method annotatedMethod,
                                                                   final Object bean) {
        return new MethodInvokingMessageConsumer<>(annotation.keyPattern(), annotation.payloadType(), bean, annotatedMethod);
    }

    private MessageReceiverEndpoint matchingMessageLogReceiverEndpointFor(final MessageLogConsumer annotation) {
        return applicationContext.getBean(annotation.endpointName(), MessageLogReceiverEndpoint.class);
    }

}
