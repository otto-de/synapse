package de.otto.edison.eventsourcing.consumer;

import de.otto.edison.eventsourcing.message.Message;
import de.otto.edison.eventsourcing.state.StateRepository;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Instant;

import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class DefaultMessageConsumerTest {

    @Mock
    private StateRepository<String> stateRepository;

    @Test
    public void shouldStoreEventInStateRepositoryOnAccept() {
        //given
        DefaultEventConsumer<String> consumer = createDefaultEventConsumer();

        //when
        consumer.accept(Message.message("someKey", "12345", "someSeqNumber", Instant.now()));

        //then
        verify(stateRepository).put("someKey", "12345");
    }

    @Test
    public void shouldRemoveEventFromStateRepositoryWhenDeletedOnAccept() {
        //given
        DefaultEventConsumer<String> consumer = createDefaultEventConsumer();

        //when
        consumer.accept(Message.message("someKey", null, "someSeqNumber", Instant.now()));

        //then
        verify(stateRepository).remove("someKey");

    }

    private DefaultEventConsumer<String> createDefaultEventConsumer() {
        return new DefaultEventConsumer<>(".*", String.class, stateRepository);
    }
}
