package de.otto.synapse.endpoint.sender.kinesis;

import de.otto.synapse.channel.selector.Kinesis;
import de.otto.synapse.channel.selector.MessageLog;
import de.otto.synapse.channel.selector.MessageQueue;
import de.otto.synapse.endpoint.MessageInterceptor;
import de.otto.synapse.endpoint.MessageInterceptorRegistration;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.sender.MessageSenderEndpoint;
import de.otto.synapse.translator.MessageFormat;
import org.junit.Test;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.ListStreamsResponse;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KinesisMessageSenderEndpointFactoryTest {

    @Test
    public void shouldBuildKinesisMessageSender() {
        final KinesisAsyncClient kinesisClient = mock(KinesisAsyncClient.class);
        when(kinesisClient.listStreams()).thenReturn(completedFuture(ListStreamsResponse.builder().build()));

        final KinesisMessageSenderEndpointFactory factory = new KinesisMessageSenderEndpointFactory(new MessageInterceptorRegistry(), kinesisClient);

        final KinesisMessageSender sender = (KinesisMessageSender) factory.create("foo-stream", MessageFormat.V2);
        assertThat(sender.getChannelName(), is("foo-stream"));
        assertThat(sender.getMessageFormat(), is(MessageFormat.V2));
        assertThat(sender, is(instanceOf(KinesisMessageSender.class)));
    }

    @Test
    public void shouldMatchMessageLogSelectors() {
        final MessageInterceptorRegistry interceptorRegistry = mock(MessageInterceptorRegistry.class);
        final KinesisAsyncClient kinesisClient = mock(KinesisAsyncClient.class);

        final KinesisMessageSenderEndpointFactory factory = new KinesisMessageSenderEndpointFactory(interceptorRegistry, kinesisClient);

        assertThat(factory.matches(MessageLog.class), is(true));
        assertThat(factory.matches(Kinesis.class), is(true));
    }

    @Test
    public void shouldNotMatchMessageQueueSelectors() {
        final MessageInterceptorRegistry interceptorRegistry = mock(MessageInterceptorRegistry.class);
        final KinesisAsyncClient kinesisClient = mock(KinesisAsyncClient.class);

        final KinesisMessageSenderEndpointFactory factory = new KinesisMessageSenderEndpointFactory(interceptorRegistry, kinesisClient);

        assertThat(factory.matches(MessageQueue.class), is(false));
    }

    @Test
    public void shouldRegisterMessageInterceptor() {
        final KinesisAsyncClient kinesisClient = mock(KinesisAsyncClient.class);

        final MessageInterceptorRegistry registry = new MessageInterceptorRegistry();
        final MessageInterceptor interceptor = mock(MessageInterceptor.class);
        registry.register(MessageInterceptorRegistration.allChannelsWith(interceptor));

        final KinesisMessageSenderEndpointFactory factory = new KinesisMessageSenderEndpointFactory(registry, kinesisClient);

        final MessageSenderEndpoint sender = factory.create("foo-stream", MessageFormat.V1);

        assertThat(sender.getInterceptorChain().getInterceptors(), contains(interceptor));
    }
}