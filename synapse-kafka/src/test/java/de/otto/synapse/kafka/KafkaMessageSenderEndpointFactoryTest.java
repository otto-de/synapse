package de.otto.synapse.kafka;

import de.otto.synapse.channel.selector.Kafka;
import de.otto.synapse.channel.selector.MessageLog;
import de.otto.synapse.channel.selector.MessageQueue;
import de.otto.synapse.endpoint.MessageInterceptor;
import de.otto.synapse.endpoint.MessageInterceptorRegistration;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.sender.MessageSenderEndpoint;
import de.otto.synapse.endpoint.sender.kafka.KafkaMessageSender;
import de.otto.synapse.endpoint.sender.kafka.KafkaMessageSenderEndpointFactory;
import de.otto.synapse.translator.MessageFormat;
import org.junit.Test;
import org.springframework.kafka.core.KafkaTemplate;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.mock;

public class KafkaMessageSenderEndpointFactoryTest {

    @Test
    public void shouldBuildKinesisMessageSender() {
        final KafkaTemplate kafkaTemplate = mock(KafkaTemplate.class);

        final KafkaMessageSenderEndpointFactory factory = new KafkaMessageSenderEndpointFactory(new MessageInterceptorRegistry(), kafkaTemplate);

        final KafkaMessageSender sender = (KafkaMessageSender) factory.create("foo-stream", MessageFormat.V1);
        assertThat(sender.getChannelName(), is("foo-stream"));
        assertThat(sender.getMessageFormat(), is(MessageFormat.V1));
        assertThat(sender, is(instanceOf(KafkaMessageSender.class)));
    }

    @Test
    public void shouldMatchMessageLogSelectors() {
        final MessageInterceptorRegistry interceptorRegistry = mock(MessageInterceptorRegistry.class);
        final KafkaTemplate kafkaTemplate = mock(KafkaTemplate.class);

        final KafkaMessageSenderEndpointFactory factory = new KafkaMessageSenderEndpointFactory(interceptorRegistry, kafkaTemplate);

        assertThat(factory.matches(MessageLog.class), is(true));
        assertThat(factory.matches(Kafka.class), is(true));
    }

    @Test
    public void shouldNotMatchMessageQueueSelectors() {
        final MessageInterceptorRegistry interceptorRegistry = mock(MessageInterceptorRegistry.class);
        final KafkaTemplate kafkaTemplate = mock(KafkaTemplate.class);

        final KafkaMessageSenderEndpointFactory factory = new KafkaMessageSenderEndpointFactory(interceptorRegistry, kafkaTemplate);

        assertThat(factory.matches(MessageQueue.class), is(false));
    }

    @Test
    public void shouldRegisterMessageInterceptor() {
        final KafkaTemplate kafkaTemplate = mock(KafkaTemplate.class);

        final MessageInterceptorRegistry registry = new MessageInterceptorRegistry();
        final MessageInterceptor interceptor = mock(MessageInterceptor.class);
        registry.register(MessageInterceptorRegistration.allChannelsWith(interceptor));

        final KafkaMessageSenderEndpointFactory factory = new KafkaMessageSenderEndpointFactory(registry, kafkaTemplate);

        final MessageSenderEndpoint sender = factory.create("foo-stream", MessageFormat.V1);

        assertThat(sender.getInterceptorChain().getInterceptors(), contains(interceptor));
    }
}