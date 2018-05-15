package de.otto.synapse.eventsource.aws;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.endpoint.MessageInterceptor;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.eventsource.EventSource;
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

public class KinesisEventSourceBuilderTest {


    @Test
    public void shouldBuildEventSource() {
        // given
        final ObjectMapper objectMapper = new ObjectMapper();
        final ApplicationEventPublisher eventPublisher = mock(ApplicationEventPublisher.class);
        final MessageInterceptorRegistry registry = mock(MessageInterceptorRegistry.class);
        final KinesisClient kinesisClient = someKinesisClient();
        final KinesisEventSourceBuilder builder = new KinesisEventSourceBuilder(objectMapper, eventPublisher, kinesisClient, registry);
        // when
        final EventSource eventSource = builder.buildEventSource("Horst", "some-channel");
        // then
        assertThat(eventSource, is(instanceOf(KinesisEventSource.class)));
        assertThat(eventSource.getName(), is("Horst"));
        assertThat(eventSource.getChannelName(), is("some-channel"));
        assertThat(eventSource.getMessageDispatcher(), is(notNullValue()));
    }

    @Test
    public void shouldRegisterInterceptors() {
        // given
        final ObjectMapper objectMapper = new ObjectMapper();
        final ApplicationEventPublisher eventPublisher = mock(ApplicationEventPublisher.class);
        final MessageInterceptor interceptor = mock(MessageInterceptor.class);
        final MessageInterceptorRegistry registry = new MessageInterceptorRegistry();
        registry.register(matchingReceiverChannelsWith("some-channel", interceptor));
        final KinesisClient kinesisClient = someKinesisClient();
        final KinesisEventSourceBuilder builder = new KinesisEventSourceBuilder(objectMapper, eventPublisher, kinesisClient, registry);
        // when
        final KinesisEventSource eventSource = (KinesisEventSource) builder.buildEventSource("Horst", "some-channel");
        // then
        assertThat(eventSource.getMessageLogReceiverEndpoint().getInterceptorChain().getInterceptors(), contains(interceptor));

    }

    @Test
    public void shouldRegisterOnlyReceiverInterceptors() {
        // given
        final ObjectMapper objectMapper = new ObjectMapper();
        final ApplicationEventPublisher eventPublisher = mock(ApplicationEventPublisher.class);
        final MessageInterceptor receiverInterceptor = mock(MessageInterceptor.class);
        final MessageInterceptor senderInterceptor = mock(MessageInterceptor.class);
        final MessageInterceptorRegistry registry = new MessageInterceptorRegistry();
        registry.register(matchingReceiverChannelsWith("some-channel", receiverInterceptor));
        registry.register(matchingSenderChannelsWith("some-channel", senderInterceptor));
        final KinesisClient kinesisClient = someKinesisClient();
        final KinesisEventSourceBuilder builder = new KinesisEventSourceBuilder(objectMapper, eventPublisher, kinesisClient, registry);
        // when
        final KinesisEventSource eventSource = (KinesisEventSource) builder.buildEventSource("Horst", "some-channel");
        // then
        assertThat(eventSource.getMessageLogReceiverEndpoint().getInterceptorChain().getInterceptors(), contains(receiverInterceptor));

    }

    @Test
    public void shouldRegisterOnlyReceiverInterceptorsMatchingChannelName() {
        // given
        final ObjectMapper objectMapper = new ObjectMapper();
        final ApplicationEventPublisher eventPublisher = mock(ApplicationEventPublisher.class);
        final MessageInterceptor someInterceptor = mock(MessageInterceptor.class);
        final MessageInterceptor someOtherInterceptor = mock(MessageInterceptor.class);
        final MessageInterceptorRegistry registry = new MessageInterceptorRegistry();
        registry.register(matchingChannelsWith("some-channel", someInterceptor));
        registry.register(matchingChannelsWith("some-other-channel", someOtherInterceptor));
        final KinesisClient kinesisClient = someKinesisClient();
        final KinesisEventSourceBuilder builder = new KinesisEventSourceBuilder(objectMapper, eventPublisher, kinesisClient, registry);
        // when
        final KinesisEventSource eventSource = (KinesisEventSource) builder.buildEventSource("Horst", "some-channel");
        // then
        assertThat(eventSource.getMessageLogReceiverEndpoint().getInterceptorChain().getInterceptors(), contains(someInterceptor));
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