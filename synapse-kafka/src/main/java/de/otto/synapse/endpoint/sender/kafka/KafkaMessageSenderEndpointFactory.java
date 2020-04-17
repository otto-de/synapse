package de.otto.synapse.endpoint.sender.kafka;

import de.otto.synapse.channel.selector.Kafka;
import de.otto.synapse.channel.selector.Selector;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.sender.MessageSenderEndpoint;
import de.otto.synapse.endpoint.sender.MessageSenderEndpointFactory;
import de.otto.synapse.translator.MessageFormat;
import de.otto.synapse.translator.TextMessageTranslator;
import org.springframework.kafka.core.KafkaTemplate;

import javax.annotation.Nonnull;

public class KafkaMessageSenderEndpointFactory implements MessageSenderEndpointFactory {

    private final MessageInterceptorRegistry registry;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaMessageSenderEndpointFactory(final MessageInterceptorRegistry registry,
                                             final KafkaTemplate<String, String> kafkaTemplate) {
        this.registry = registry;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public MessageSenderEndpoint create(final @Nonnull String channelName, final MessageFormat messageFormat) {
        return new KafkaMessageSender(channelName, registry, new TextMessageTranslator(), kafkaTemplate);
    }

    @Override
    public boolean matches(final Class<? extends Selector> channelSelector) {
        return channelSelector.isAssignableFrom(selector());
    }

    @Override
    public Class<? extends Selector> selector() {
        return Kafka.class;
    }

}
