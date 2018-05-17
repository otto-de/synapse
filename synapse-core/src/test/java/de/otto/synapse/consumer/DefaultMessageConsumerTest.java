package de.otto.synapse.consumer;

import de.otto.synapse.message.Message;
import de.otto.synapse.state.StateRepository;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Instant;

import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static de.otto.synapse.message.Header.responseHeader;
import static java.time.Duration.ZERO;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class DefaultMessageConsumerTest {

    @Mock
    private StateRepository<String> stateRepository;

    @Test
    public void shouldStoreEventInStateRepositoryOnAccept() {
        //given
        DefaultMessageConsumer<String> consumer = createDefaultEventConsumer();

        //when
        consumer.accept(Message.message(
                "someKey",
                responseHeader(fromPosition("some-shard", ZERO, "someSeqNumber"), Instant.now()),
                "12345"
        ));

        //then
        verify(stateRepository).put("someKey", "12345");
    }

    @Test
    public void shouldRemoveEventFromStateRepositoryWhenDeletedOnAccept() {
        //given
        DefaultMessageConsumer<String> consumer = createDefaultEventConsumer();

        //when
        consumer.accept(Message.message(
                "someKey",
                responseHeader(fromPosition("some-shard", ZERO, "someSeqNumber"), Instant.now()),
                null
        ));

        //then
        verify(stateRepository).remove("someKey");

    }

    private DefaultMessageConsumer<String> createDefaultEventConsumer() {
        return new DefaultMessageConsumer<>(".*", String.class, stateRepository);
    }
}
