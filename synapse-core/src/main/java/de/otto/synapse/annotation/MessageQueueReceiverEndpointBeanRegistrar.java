package de.otto.synapse.annotation;

import de.otto.synapse.endpoint.receiver.DelegateMessageQueueReceiverEndpoint;
import org.slf4j.Logger;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.annotation.AnnotationAttributes;

import java.lang.annotation.Annotation;

import static java.lang.String.format;
import static org.slf4j.LoggerFactory.getLogger;
import static org.springframework.beans.factory.support.AbstractBeanDefinition.DEPENDENCY_CHECK_ALL;
import static org.springframework.beans.factory.support.BeanDefinitionBuilder.genericBeanDefinition;

/**
 * {@link ImportBeanDefinitionRegistrar} for message log support.
 *
 * @see EnableMessageQueueReceiverEndpoint
 */
public class MessageQueueReceiverEndpointBeanRegistrar extends AbstractAnnotationBasedBeanRegistrar {

    private static final Logger LOG = getLogger(MessageQueueReceiverEndpointBeanRegistrar.class);


    @Override
    protected Class<? extends Annotation> getAnnotationType() {
        return EnableMessageQueueReceiverEndpoint.class;
    }

    @Override
    protected void registerBeanDefinitions(final String channelName,
                                           final String beanName,
                                           final AnnotationAttributes annotationAttributes,
                                           final BeanDefinitionRegistry registry) {
        if (!registry.containsBeanDefinition(beanName)) {
            registry.registerBeanDefinition(
                    beanName,
                    genericBeanDefinition(DelegateMessageQueueReceiverEndpoint.class)
                            .addConstructorArgValue(channelName)
                            .setDependencyCheck(DEPENDENCY_CHECK_ALL)
                            .getBeanDefinition()
            );

            LOG.info("Registered MessageQueueReceiverEndpoint {} with for channelName {}", beanName, channelName);
        } else {
            throw new BeanCreationException(beanName, format("MessageQueueReceiverEndpoint %s is already registered.", beanName));
        }
    }

}
