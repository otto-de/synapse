package de.otto.synapse.endpoint.sender.aws;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.endpoint.sender.MessageSenderEndpoint;
import org.junit.Test;
import software.amazon.awssdk.services.kinesis.KinesisClient;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class KinesisMessageSenderFactoryTest {

    @Test
    public void shouldBuildKinesisMessageSender() {
        final ObjectMapper objectMapper = mock(ObjectMapper.class);
        final KinesisClient kinesisClient = mock(KinesisClient.class);

        final KinesisMessageSenderFactory factory = new KinesisMessageSenderFactory(objectMapper, kinesisClient);

        final MessageSenderEndpoint sender = factory.createSenderForStream("foo-stream");
        assertThat(sender.getChannelName(), is("foo-stream"));
        assertThat(sender, is(instanceOf(KinesisMessageSender.class)));
    }
}