package de.otto.synapse.edison.statusdetail;

import de.otto.edison.status.domain.Status;
import de.otto.edison.status.domain.StatusDetail;
import de.otto.edison.testsupport.util.TestClock;
import de.otto.synapse.edison.provider.MessageReceiverEndpointInfoProvider;
import de.otto.synapse.info.MessageReceiverEndpointInfo;
import de.otto.synapse.info.MessageReceiverEndpointInfos;
import org.junit.Test;

import java.util.List;
import java.util.stream.Stream;

import static de.otto.synapse.info.MessageEndpointStatus.*;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EventSourcingStatusDetailIndicatorTest {

    private TestClock testClock = TestClock.now();

    public EventSourcingStatusDetailIndicatorTest() {
    }

    private EventSourcingStatusDetailIndicator detailIndicatorWith(final MessageReceiverEndpointInfo... infos) {
        MessageReceiverEndpointInfoProvider provider = mock(MessageReceiverEndpointInfoProvider.class);
        MessageReceiverEndpointInfos endpointInfos = mock(MessageReceiverEndpointInfos.class);
        when(endpointInfos.stream()).thenReturn(Stream.of(infos));
        when(provider.getInfos()).thenReturn(endpointInfos);
        return new EventSourcingStatusDetailIndicator(provider);
    }

    @Test
    public void shouldBeOkForSnapshotStarting() {
        //given
        //when
        EventSourcingStatusDetailIndicator eventSourcingStatusDetailIndicator = detailIndicatorWith(
                MessageReceiverEndpointInfo.builder()
                        .withChannelName("myChannelName")
                        .withMessage("Loading snapshot")
                        .withStatus(STARTING)
                        .build()
        );

        //then
        List<StatusDetail> statusDetail = eventSourcingStatusDetailIndicator.statusDetails();

        assertThat(statusDetail.get(0).getStatus(), is(Status.OK));
        assertThat(statusDetail.get(0).getName(), is("myChannelName"));
        assertThat(statusDetail.get(0).getMessage(), is("Loading snapshot"));
    }

    @Test
    public void shouldHaveErrorIfSomethingFailed() {
        //given

        //when
        EventSourcingStatusDetailIndicator eventSourcingStatusDetailIndicator = detailIndicatorWith(
                MessageReceiverEndpointInfo.builder()
                        .withChannelName("myChannelName")
                        .withMessage("Kawumm")
                        .withStatus(FAILED)
                        .build(),
                MessageReceiverEndpointInfo.builder()
                        .withChannelName("myChannelName2")
                        .withMessage("Loading snapshot")
                        .withStatus(RUNNING)
                        .build()

        );

        //then
        List<StatusDetail> statusDetails = eventSourcingStatusDetailIndicator.statusDetails();

        assertThat(statusDetails.get(0).getStatus(), is(Status.ERROR));
        assertThat(statusDetails.get(0).getName(), is("myChannelName"));
        assertThat(statusDetails.get(0).getMessage(), is("Kawumm"));
        assertThat(statusDetails.get(1).getStatus(), is(Status.OK));
        assertThat(statusDetails.get(1).getName(), is("myChannelName2"));
        assertThat(statusDetails.get(1).getMessage(), is("Loading snapshot"));
    }

}