package de.otto.synapse.endpoint.sender.aws;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.endpoint.MessageInterceptor;
import de.otto.synapse.endpoint.MessageInterceptorRegistration;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.sender.MessageSenderEndpoint;
import org.junit.Test;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.ListStreamsResponse;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KinesisMessageSenderEndpointFactoryTest {

    @Test
    public void shouldBuildKinesisMessageSender() {
        final ObjectMapper objectMapper = mock(ObjectMapper.class);
        final KinesisClient kinesisClient = mock(KinesisClient.class);
        when(kinesisClient.listStreams()).thenReturn(ListStreamsResponse.builder().build());

        final KinesisMessageSenderEndpointFactory factory = new KinesisMessageSenderEndpointFactory(new MessageInterceptorRegistry(), objectMapper, kinesisClient);

        final MessageSenderEndpoint sender = factory.create("foo-stream");
        assertThat(sender.getChannelName(), is("foo-stream"));
        assertThat(sender, is(instanceOf(KinesisMessageSender.class)));
    }

    @Test
    public void shouldNotSupportMissingChannel() {
        final ObjectMapper objectMapper = mock(ObjectMapper.class);
        final KinesisClient kinesisClient = mock(KinesisClient.class);
        when(kinesisClient.listStreams()).thenReturn(ListStreamsResponse.builder().build());

        final KinesisMessageSenderEndpointFactory factory = new KinesisMessageSenderEndpointFactory(new MessageInterceptorRegistry(), objectMapper, kinesisClient);

        assertThat(factory.supportsChannel("foo-stream"), is(false));
    }

    @Test
    public void shouldSupportExistingChannel() {
        final ObjectMapper objectMapper = mock(ObjectMapper.class);
        final KinesisClient kinesisClient = mock(KinesisClient.class);
        when(kinesisClient.listStreams()).thenReturn(ListStreamsResponse.builder().streamNames("foo-stream").build());

        final KinesisMessageSenderEndpointFactory factory = new KinesisMessageSenderEndpointFactory(new MessageInterceptorRegistry(), objectMapper, kinesisClient);

        assertThat(factory.supportsChannel("foo-stream"), is(true));
    }

    @Test
    public void shouldRegisterMessageInterceptor() {
        final ObjectMapper objectMapper = mock(ObjectMapper.class);
        final KinesisClient kinesisClient = mock(KinesisClient.class);
        when(kinesisClient.listStreams()).thenReturn(ListStreamsResponse.builder().build());

        final MessageInterceptorRegistry registry = new MessageInterceptorRegistry();
        final MessageInterceptor interceptor = mock(MessageInterceptor.class);
        registry.register(MessageInterceptorRegistration.allChannelsWith(interceptor));

        final KinesisMessageSenderEndpointFactory factory = new KinesisMessageSenderEndpointFactory(registry, objectMapper, kinesisClient);

        final MessageSenderEndpoint sender = factory.create("foo-stream");

        assertThat(sender.getInterceptorChain().getInterceptors(), contains(interceptor));
    }
}