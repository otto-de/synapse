package de.otto.synapse.channel.aws;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.endpoint.MessageInterceptor;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpoint;
import org.junit.Test;
import org.springframework.context.ApplicationEventPublisher;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.*;

import static de.otto.synapse.endpoint.MessageInterceptorRegistration.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KinesisMessageLogReceiverEndpointFactoryTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ApplicationEventPublisher eventPublisher = mock(ApplicationEventPublisher.class);
    private final MessageInterceptorRegistry registry = mock(MessageInterceptorRegistry.class);
    private final KinesisClient kinesisClient = someKinesisClient();

    @Test
    public void shouldBuildEventSource() {
        // given
        final KinesisMessageLogReceiverEndpointFactory factory = new KinesisMessageLogReceiverEndpointFactory(registry, kinesisClient, objectMapper, eventPublisher);
        // when
        final MessageLogReceiverEndpoint endpoint = factory.create("some-channel");
        // then
        assertThat(endpoint, is(instanceOf(KinesisMessageLogReceiverEndpoint.class)));
        assertThat(endpoint.getChannelName(), is("some-channel"));
        assertThat(endpoint.getMessageDispatcher(), is(notNullValue()));
    }

    @Test
    public void shouldRegisterInterceptors() {
        // given
        final MessageInterceptor interceptor = mock(MessageInterceptor.class);
        final MessageInterceptorRegistry registry = new MessageInterceptorRegistry();
        // when
        registry.register(matchingReceiverChannelsWith("some-channel", interceptor));
        final KinesisMessageLogReceiverEndpointFactory factory = new KinesisMessageLogReceiverEndpointFactory(registry, kinesisClient, objectMapper, eventPublisher);
        final MessageLogReceiverEndpoint endpoint = factory.create("some-channel");
        // then
        assertThat(endpoint.getInterceptorChain().getInterceptors(), contains(interceptor));

    }

    @Test
    public void shouldRegisterOnlyReceiverInterceptors() {
        // given
        final MessageInterceptorRegistry registry = new MessageInterceptorRegistry();
        final MessageInterceptor receiverInterceptor = mock(MessageInterceptor.class);
        final MessageInterceptor senderInterceptor = mock(MessageInterceptor.class);
        // when
        registry.register(matchingReceiverChannelsWith("some-channel", receiverInterceptor));
        registry.register(matchingSenderChannelsWith("some-channel", senderInterceptor));
        final KinesisMessageLogReceiverEndpointFactory factory = new KinesisMessageLogReceiverEndpointFactory(registry, kinesisClient, objectMapper, eventPublisher);
        final MessageLogReceiverEndpoint endpoint = factory.create("some-channel");

        // then
        assertThat(endpoint.getInterceptorChain().getInterceptors(), contains(receiverInterceptor));

    }

    @Test
    public void shouldRegisterOnlyReceiverInterceptorsMatchingChannelName() {
        // given
        final MessageInterceptorRegistry registry = new MessageInterceptorRegistry();
        final MessageInterceptor someInterceptor = mock(MessageInterceptor.class);
        final MessageInterceptor someOtherInterceptor = mock(MessageInterceptor.class);
        // when
        registry.register(matchingChannelsWith("some-channel", someInterceptor));
        registry.register(matchingChannelsWith("some-other-channel", someOtherInterceptor));
        final KinesisMessageLogReceiverEndpointFactory factory = new KinesisMessageLogReceiverEndpointFactory(registry, kinesisClient, objectMapper, eventPublisher);
        final MessageLogReceiverEndpoint endpoint = factory.create("some-channel");

        // then
        assertThat(endpoint.getInterceptorChain().getInterceptors(), contains(someInterceptor));
    }

    private KinesisClient someKinesisClient() {
        final KinesisClient kinesisClient = mock(KinesisClient.class);
        describeStreamResponse(kinesisClient);
        return kinesisClient;
    }

    private void describeStreamResponse(final KinesisClient kinesisClient) {
        DescribeStreamResponse response = createResponseForShards(
                Shard.builder()
                        .shardId("foo")
                        .sequenceNumberRange(SequenceNumberRange.builder()
                                .startingSequenceNumber("42")
                                .endingSequenceNumber("4711")
                                .build())
                        .build()
        );

        when(kinesisClient.describeStream(any(DescribeStreamRequest.class))).thenReturn(response);
    }

    private DescribeStreamResponse createResponseForShards(Shard shard) {
        return DescribeStreamResponse.builder()
                .streamDescription(StreamDescription.builder()
                        .shards(shard)
                        .hasMoreShards(false)
                        .build())
                .build();
    }
}