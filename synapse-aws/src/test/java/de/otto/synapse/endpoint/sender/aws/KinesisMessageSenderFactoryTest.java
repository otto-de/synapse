package de.otto.synapse.endpoint.sender.aws;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.endpoint.MessageInterceptor;
import de.otto.synapse.endpoint.MessageInterceptorRegistration;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.sender.MessageSenderEndpoint;
import org.junit.Test;
import software.amazon.awssdk.services.kinesis.KinesisClient;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.mock;

public class KinesisMessageSenderFactoryTest {

    @Test
    public void shouldBuildKinesisMessageSender() {
        final ObjectMapper objectMapper = mock(ObjectMapper.class);
        final KinesisClient kinesisClient = mock(KinesisClient.class);

        final KinesisMessageSenderFactory factory = new KinesisMessageSenderFactory(new MessageInterceptorRegistry(), objectMapper, kinesisClient);

        final MessageSenderEndpoint sender = factory.createSenderFor("foo-stream");
        assertThat(sender.getChannelName(), is("foo-stream"));
        assertThat(sender, is(instanceOf(KinesisMessageSender.class)));
    }

    @Test
    public void shouldRegisterMessageInterceptor() {
        final ObjectMapper objectMapper = mock(ObjectMapper.class);
        final KinesisClient kinesisClient = mock(KinesisClient.class);

        final MessageInterceptorRegistry registry = new MessageInterceptorRegistry();
        final MessageInterceptor interceptor = mock(MessageInterceptor.class);
        registry.register(MessageInterceptorRegistration.allChannelsWith(interceptor));

        final KinesisMessageSenderFactory factory = new KinesisMessageSenderFactory(registry, objectMapper, kinesisClient);

        final MessageSenderEndpoint sender = factory.createSenderFor("foo-stream");

        assertThat(sender.getInterceptorChain().getInterceptors(), contains(interceptor));
    }
}