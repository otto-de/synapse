package de.otto.synapse.annotation;

import de.otto.synapse.channel.selector.Selector;
import de.otto.synapse.endpoint.sender.DelegateMessageSenderEndpoint;
import de.otto.synapse.translator.MessageFormat;
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
public class MessageSenderEndpointBeanRegistrar extends AbstractAnnotationBasedBeanRegistrar {

    private static final Logger LOG = getLogger(MessageSenderEndpointBeanRegistrar.class);

    @Override
    protected Class<? extends Annotation> getAnnotationType() {
        return EnableMessageSenderEndpoint.class;
    }

    @Override
    protected void registerBeanDefinitions(final String channelName,
                                           final String beanName,
                                           final AnnotationAttributes annotationAttributes,
                                           final BeanDefinitionRegistry registry) {
            final Class<? extends Selector> channelSelector = annotationAttributes.getClass("selector");
            final MessageFormat messageFormat = annotationAttributes.getEnum("messageFormat");

            if (!registry.containsBeanDefinition(beanName)) {
                registry.registerBeanDefinition(
                        beanName,
                        genericBeanDefinition(DelegateMessageSenderEndpoint.class)
                                .addConstructorArgValue(channelName)
                                .addConstructorArgValue(channelSelector)
                                .addConstructorArgValue(messageFormat)
                                .setDependencyCheck(DEPENDENCY_CHECK_ALL)
                                .getBeanDefinition()
                );

                LOG.info("Registered MessageQueueSenderEndpoint {} with for channelName {}, messageFormat {}", beanName, channelName, messageFormat);
            } else {
                throw new BeanCreationException(
                        beanName,
                        format("MessageQueueReceiverEndpoint %s is already registered.", beanName)
                );
            }
    }

}
