package de.otto.edison.eventsourcing.annotation;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.CaseFormat;
import de.otto.edison.eventsourcing.CompactingKinesisEventSource;
import de.otto.edison.eventsourcing.consumer.MethodInvokingEventConsumer;
import de.otto.edison.eventsourcing.s3.SnapshotConsumerService;
import de.otto.edison.eventsourcing.s3.SnapshotReadService;
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
import software.amazon.awssdk.services.kinesis.KinesisClient;

import java.lang.reflect.Method;
import java.util.*;
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
            Class<?> targetClass = AopUtils.getTargetClass(bean);
            Map<Method, Set<EventSourceConsumer>> annotatedMethods = findMethodsAnnotatedWithEventSourceConsumer(targetClass);
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

    private void registerEventConsumers(Object bean, String beanName, Map<Method, Set<EventSourceConsumer>> annotatedMethods) {
        Map<String, Class> payloadTypesForName = new HashMap<>();

        for (Map.Entry<Method, Set<EventSourceConsumer>> entry : annotatedMethods.entrySet()) {
            final Method method = entry.getKey();
            for (EventSourceConsumer consumer : entry.getValue()) {
                registerEventConsumer(consumer, method, bean);
                String resolvedStreamName = applicationContext.getEnvironment().resolvePlaceholders(consumer.streamName());

                assertThatEventConsumersForSameStreamNameHaveSamePayloadType(payloadTypesForName, consumer, resolvedStreamName);

                if (!eventSourceExists(resolvedStreamName)) {
                    registerEventSource(resolvedStreamName, consumer.payloadType());
                    payloadTypesForName.put(resolvedStreamName, consumer.payloadType());
                }
            }
        }
        LOG.info("{} @EventSourceConsumer methods processed on bean {} : {}'", annotatedMethods.size(), beanName, annotatedMethods);
    }

    private void assertThatEventConsumersForSameStreamNameHaveSamePayloadType(Map<String, Class> payloadTypesForName, EventSourceConsumer consumer, String resolvedStreamName) {
        if (payloadTypesForName.containsKey(resolvedStreamName) && payloadTypesForName.get(resolvedStreamName) != consumer.payloadType()) {
            throw new IllegalStateException(String.format("Cannot register consumers for same streamName \"%s\" but with different payloadType", resolvedStreamName));
        }
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

    private void registerEventConsumer(final EventSourceConsumer annotation,
                                       final Method annotatedMethod,
                                       final Object bean) {
        final String streamName = applicationContext.getEnvironment().resolvePlaceholders(annotation.streamName());
        final MethodInvokingEventConsumer eventConsumer = new MethodInvokingEventConsumer(streamName, bean, annotatedMethod);
        final ConfigurableListableBeanFactory beanFactory = applicationContext.getBeanFactory();
        if (!beanFactory.containsBean(annotation.name())) {
            this.applicationContext.getBeanFactory().registerSingleton(annotation.name(), eventConsumer);
        }
    }

    private <T> void registerEventSource(String streamName, Class<T> payloadType) {
        SnapshotReadService snapshotService = applicationContext.getBean(SnapshotReadService.class);
        SnapshotConsumerService snapshotConsumerService = applicationContext.getBean(SnapshotConsumerService.class);
        ObjectMapper objectMapper = applicationContext.getBean(ObjectMapper.class);
        KinesisClient kinesisClient = applicationContext.getBean(KinesisClient.class);
        CompactingKinesisEventSource<T> eventSource = new CompactingKinesisEventSource<>(streamName, payloadType, snapshotService, snapshotConsumerService, objectMapper, kinesisClient);
        applicationContext.getBeanFactory().registerSingleton(streamNameToEventSourceName(streamName), eventSource);
    }

}