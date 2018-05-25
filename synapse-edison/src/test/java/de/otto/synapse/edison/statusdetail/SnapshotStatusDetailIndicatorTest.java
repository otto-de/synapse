package de.otto.synapse.edison.statusdetail;

import de.otto.edison.status.domain.Status;
import de.otto.synapse.eventsource.EventSource;
import de.otto.synapse.info.SnapshotReaderNotification;
import org.junit.Test;

import java.util.Optional;

import static de.otto.edison.status.domain.StatusDetail.statusDetail;
import static de.otto.synapse.info.SnapshotReaderStatus.*;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SnapshotStatusDetailIndicatorTest {

    public SnapshotStatusDetailIndicatorTest() {
    }

    private SnapshotStatusDetailIndicator detailIndicatorWith(final String... channels) {
        return new SnapshotStatusDetailIndicator(channels == null
                ? Optional.empty()
                : Optional.of(stream(channels).map(this::eventSource).collect(toList()))
        );
    }

    private EventSource eventSource(final String channelName) {
        final EventSource mock = mock(EventSource.class);
        when(mock.getChannelName()).thenReturn(channelName);
        return mock;
    }

    @Test
    public void shouldBeOkForMissingSnapshot() {

        // given
        final SnapshotStatusDetailIndicator indicator = detailIndicatorWith("foo");

        // then
        assertThat(indicator.statusDetails(), contains(statusDetail("foo", Status.OK, "No snapshot info available.")));
    }

    @Test
    public void shouldBeOkForSnapshotStarting() {

        //given
        final SnapshotStatusDetailIndicator indicator = detailIndicatorWith("foo");

        //when
        indicator.on(SnapshotReaderNotification.builder()
                        .withChannelName("foo")
                        .withMessage("Loading snapshot")
                        .withStatus(STARTING)
                        .build()
        );

        //then
        assertThat(indicator.statusDetails(), contains(
                statusDetail("foo", Status.OK, "Loading snapshot"))
        );
    }

    @Test
    public void shouldHaveErrorIfSomethingFailed() {

        //given
        final SnapshotStatusDetailIndicator indicator = detailIndicatorWith("foo", "bar");

        //when
        indicator.on(SnapshotReaderNotification.builder()
                .withChannelName("foo")
                .withMessage("Snapshot Loaded")
                .withStatus(FINISHED)
                .build()
        );

        indicator.on(SnapshotReaderNotification.builder()
                .withChannelName("bar")
                .withMessage("Snapshot Failed")
                .withStatus(FAILED)
                .build()
        );

        //then
        assertThat(indicator.statusDetails(), contains(
                statusDetail("bar", Status.ERROR, "Snapshot Failed"),
                statusDetail("foo", Status.OK, "Snapshot Loaded"))
        );

    }

}