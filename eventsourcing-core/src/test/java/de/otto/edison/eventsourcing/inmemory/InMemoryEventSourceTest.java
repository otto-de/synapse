package de.otto.edison.eventsourcing.inmemory;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.eventsourcing.event.Event;
import de.otto.edison.eventsourcing.consumer.EventConsumer;
import org.junit.Test;

import java.util.regex.Pattern;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

public class InMemoryEventSourceTest {

    private ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void shouldSendEventInStreamToConsumer() throws Exception {
        // given
        InMemoryStream inMemoryStream = new InMemoryStream();
        InMemoryEventSource inMemoryEventSource = new InMemoryEventSource("some-stream", inMemoryStream, objectMapper);
        StringEventConsumer eventConsumer = new StringEventConsumer();
        inMemoryEventSource.register(eventConsumer);
        inMemoryStream.send(new Tuple<>("key", "payload"));

        // when
        inMemoryEventSource.consumeAll(event -> true);


        // then
        assertThat(eventConsumer.event.getEventBody().getKey(), is("key"));
        assertThat(eventConsumer.event.getEventBody().getPayload(), is("payload"));
    }

    private static class StringEventConsumer implements EventConsumer<String> {
        private Event<String> event;

        @Override
        public Class<String> payloadType() {
            return String.class;
        }

        @Override
        public Pattern keyPattern() {
            return Pattern.compile(".*");
        }

        @Override
        public void accept(Event<String> event) {
            this.event = event;
        }
    }
}
