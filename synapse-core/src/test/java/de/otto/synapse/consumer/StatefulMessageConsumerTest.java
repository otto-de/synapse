package de.otto.synapse.consumer;

import de.otto.synapse.message.Message;
import de.otto.synapse.state.StateRepository;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static de.otto.synapse.message.Header.of;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class StatefulMessageConsumerTest {

    @Mock
    private StateRepository<String> stateRepository;

    @Test
    public void shouldStoreMessageInStateRepositoryOnAccept() {
        //given
        final MessageConsumer<String> consumer = statefulMessageConsumer();

        //when
        consumer.accept(Message.message(
                "someKey",
                of(fromPosition("some-shard", "someSeqNumber")),
                "12345"
        ));

        //then
        verify(stateRepository).put("someKey", "12345");
    }

    @Test
    public void shouldTransformMessage() {
        //given
        final MessageConsumer<String> consumer = new StatefulMessageConsumer<>(".*", String.class, stateRepository, (m) -> "something completely different");

        //when
        consumer.accept(Message.message(
                "someKey",
                of(fromPosition("some-shard", "someSeqNumber")),
                "12345"
        ));

        //then
        verify(stateRepository).put("someKey", "something completely different");
    }

    @Test
    public void shouldTransformKey() {
        //given
        final MessageConsumer<String> consumer = new StatefulMessageConsumer<>(".*", String.class, stateRepository, Message::getPayload, (m) -> "someOtherKey");

        //when
        consumer.accept(Message.message(
                "someKey",
                of(fromPosition("some-shard", "someSeqNumber")),
                "12345"
        ));

        //then
        verify(stateRepository).put("someOtherKey", "12345");
    }

    @Test
    public void shouldRemoveMessageFromStateRepositoryWhenDeletedOnAccept() {
        //given
        final MessageConsumer<String> consumer = statefulMessageConsumer();

        //when
        consumer.accept(Message.message(
                "someKey",
                of(fromPosition("some-shard", "someSeqNumber")),
                null
        ));

        //then
        verify(stateRepository).remove("someKey");

    }

    private StatefulMessageConsumer<String, String> statefulMessageConsumer() {
        return new StatefulMessageConsumer<>(".*", String.class, stateRepository, Message::getPayload);
    }
}
