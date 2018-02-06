package de.otto.edison.eventsourcing.inmemory;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.edison.eventsourcing.consumer.EventConsumer;
import de.otto.edison.eventsourcing.consumer.EventSourceNotification;
import de.otto.edison.eventsourcing.consumer.StreamPosition;
import de.otto.edison.eventsourcing.event.Event;
import de.otto.edison.eventsourcing.event.EventBody;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.context.ApplicationEventPublisher;

import java.util.regex.Pattern;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class InMemoryEventSourceTest {

    private final ApplicationEventPublisher eventPublisher = mock(ApplicationEventPublisher.class);
    private ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void shouldSendEventInStreamToConsumer() {
        // given
        InMemoryStream inMemoryStream = new InMemoryStream();
        InMemoryEventSource inMemoryEventSource = new InMemoryEventSource("some-stream", inMemoryStream, eventPublisher, objectMapper);
        StringEventConsumer eventConsumer = new StringEventConsumer();
        inMemoryEventSource.register(eventConsumer);
        inMemoryStream.send(EventBody.eventBody("key", "payload"));

        // when
        inMemoryEventSource.consumeAll(event -> true);


        // then
        assertThat(eventConsumer.event.getEventBody().getKey(), is("key"));
        assertThat(eventConsumer.event.getEventBody().getPayload(), is("payload"));
    }

    @Test
    public void shouldPublishStartedAndFinishedEvents() {
        // given
        InMemoryStream inMemoryStream = new InMemoryStream();
        InMemoryEventSource inMemoryEventSource = new InMemoryEventSource("some-stream", inMemoryStream, eventPublisher, objectMapper);
        StringEventConsumer eventConsumer = new StringEventConsumer();
        inMemoryEventSource.register(eventConsumer);
        inMemoryStream.send(EventBody.eventBody("key", "payload"));

        // when
        inMemoryEventSource.consumeAll(event -> true);


        // then
        ArgumentCaptor<EventSourceNotification> notificationArgumentCaptor = ArgumentCaptor.forClass(EventSourceNotification.class);
        verify(eventPublisher, times(2)).publishEvent(notificationArgumentCaptor.capture());

        EventSourceNotification startedEvent = notificationArgumentCaptor.getAllValues().get(0);
        assertThat(startedEvent.getStatus(), is(EventSourceNotification.Status.STARTED));
        assertThat(startedEvent.getStreamPosition(), is(StreamPosition.of()));
        assertThat(startedEvent.getEventSource(), is(inMemoryEventSource));

        EventSourceNotification finishedEvent = notificationArgumentCaptor.getAllValues().get(1);
        assertThat(finishedEvent.getStatus(), is(EventSourceNotification.Status.FINISHED));
        assertThat(finishedEvent.getStreamPosition(), is(nullValue()));
        assertThat(finishedEvent.getEventSource(), is(inMemoryEventSource));
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
