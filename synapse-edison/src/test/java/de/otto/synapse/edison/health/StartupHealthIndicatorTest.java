package de.otto.synapse.edison.health;

import de.otto.synapse.eventsource.EventSource;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class StartupHealthIndicatorTest extends AbstractChannelHealthIndicatorTest {


    @Override
    protected AbstractChannelHealthIndicator createHealthIndicator(List<String> channelNames) {
        List<EventSource> eventSourceList = channelNames
                .stream()
                .map(this::mockEventSource)
                .collect(Collectors.toList());
        return new StartupHealthIndicator(Optional.of(eventSourceList));
    }

    private EventSource mockEventSource(final String channelName) {
        EventSource eventSource = mock(EventSource.class);
        when(eventSource.getChannelName()).thenReturn(channelName);
        return eventSource;
    }
}
