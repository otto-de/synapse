package de.otto.synapse.annotation.messagequeue;

import de.otto.synapse.consumer.MethodInvokingMessageConsumer;
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

public class MessageQueueConsumerBeanPostProcessor implements BeanPostProcessor, Ordered, ApplicationContextAware {

    private static final Logger LOG = getLogger(MessageQueueConsumerBeanPostProcessor.class);

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
            final Map<Method, Set<MessageQueueConsumer>> annotatedMethods = findMethodsAnnotatedWithMessageQueueConsumer(targetClass);
            if (annotatedMethods.isEmpty()) {
                this.nonAnnotatedClasses.add(bean.getClass());
                LOG.trace("No @MessageQueueConsumer annotations found on bean type: {}", bean.getClass());
            } else {
                registerMessageQueueConsumers(bean, beanName, annotatedMethods);
            }
        }
        return bean;
    }

    @Override
    public Object postProcessAfterInitialization(final Object bean, final String beanName) {
        return bean;
    }

    private Map<Method, Set<MessageQueueConsumer>> findMethodsAnnotatedWithMessageQueueConsumer(Class<?> targetClass) {
        return selectMethods(targetClass,
                (MethodIntrospector.MetadataLookup<Set<MessageQueueConsumer>>) method -> {
                    final Set<MessageQueueConsumer> consumerAnnotations = consumerAnnotationsOf(method);
                    return (!consumerAnnotations.isEmpty() ? consumerAnnotations : null);
                });
    }

    private void registerMessageQueueConsumers(final Object bean,
                                               final String beanName,
                                               final Map<Method, Set<MessageQueueConsumer>> annotatedMethods) {
        for (Map.Entry<Method, Set<MessageQueueConsumer>> entry : annotatedMethods.entrySet()) {
            final Method method = entry.getKey();
            for (MessageQueueConsumer consumerAnnotation : entry.getValue()) {
                matchingMessageQueueReceiverEndpointFor(consumerAnnotation)
                        .register(messageQueueConsumerFor(consumerAnnotation, method, bean));
            }
        }
        LOG.info("{} @MessageQueueConsumer methods processed on bean {} : {}'", annotatedMethods.size(), beanName, annotatedMethods);
    }

    /*
     * AnnotationUtils.getRepeatableAnnotations does not look at interfaces
     */
    private Set<MessageQueueConsumer> consumerAnnotationsOf(final Method method) {
        Set<MessageQueueConsumer> listeners = new HashSet<>();
        MessageQueueConsumer ann = AnnotationUtils.findAnnotation(method, MessageQueueConsumer.class);
        if (ann != null) {
            listeners.add(ann);
        }
        return listeners;
    }

    private MethodInvokingMessageConsumer<?> messageQueueConsumerFor(final MessageQueueConsumer annotation,
                                                                     final Method annotatedMethod,
                                                                     final Object bean) {
        return new MethodInvokingMessageConsumer<>(annotation.keyPattern(), annotation.payloadType(), bean, annotatedMethod);
    }

    private MessageReceiverEndpoint matchingMessageQueueReceiverEndpointFor(final MessageQueueConsumer annotation) {
        return applicationContext.getBean(annotation.endpointName(), MessageReceiverEndpoint.class);
    }

}
