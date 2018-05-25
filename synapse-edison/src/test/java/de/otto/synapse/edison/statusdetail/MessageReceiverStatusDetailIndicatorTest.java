package de.otto.synapse.edison.statusdetail;

import de.otto.edison.status.domain.Status;
import de.otto.edison.status.domain.StatusDetail;
import de.otto.synapse.edison.provider.MessageReceiverEndpointInfoProvider;
import de.otto.synapse.info.MessageReceiverEndpointInfo;
import de.otto.synapse.info.MessageReceiverEndpointInfos;
import org.junit.Test;

import java.util.List;
import java.util.stream.Stream;

import static de.otto.synapse.info.MessageReceiverEndpointInfo.builder;
import static de.otto.synapse.info.MessageReceiverStatus.*;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MessageReceiverStatusDetailIndicatorTest {

    public MessageReceiverStatusDetailIndicatorTest() {
    }

    private MessageReceiverStatusDetailIndicator detailIndicatorWith(final MessageReceiverEndpointInfo... infos) {
        MessageReceiverEndpointInfoProvider provider = mock(MessageReceiverEndpointInfoProvider.class);
        MessageReceiverEndpointInfos endpointInfos = mock(MessageReceiverEndpointInfos.class);
        when(endpointInfos.stream()).thenReturn(Stream.of(infos));
        when(provider.getInfos()).thenReturn(endpointInfos);
        return new MessageReceiverStatusDetailIndicator(provider);
    }

    @Test
    public void shouldBeOkForStartingMessageLog() {
        //given
        //when
        MessageReceiverStatusDetailIndicator indicator = detailIndicatorWith(
                builder()
                        .withChannelName("myChannelName")
                        .withMessage("some message")
                        .withStatus(STARTING)
                        .build()
        );

        //then
        List<StatusDetail> details = indicator.statusDetails();

        assertThat(details.get(0).getStatus(), is(Status.OK));
        assertThat(details.get(0).getName(), is("myChannelName"));
        assertThat(details.get(0).getMessage(), is("some message"));
    }

    @Test
    public void shouldHaveErrorIfSomethingFailed() {
        //given

        //when
        MessageReceiverStatusDetailIndicator details = detailIndicatorWith(
                builder()
                        .withChannelName("myChannelName")
                        .withMessage("Kawumm")
                        .withStatus(FAILED)
                        .build(),
                builder()
                        .withChannelName("myChannelName2")
                        .withMessage("some message")
                        .withStatus(RUNNING)
                        .build()

        );

        //then
        List<StatusDetail> statusDetails = details.statusDetails();

        assertThat(statusDetails.get(0).getStatus(), is(Status.ERROR));
        assertThat(statusDetails.get(0).getName(), is("myChannelName"));
        assertThat(statusDetails.get(0).getMessage(), is("Kawumm"));
        assertThat(statusDetails.get(1).getStatus(), is(Status.OK));
        assertThat(statusDetails.get(1).getName(), is("myChannelName2"));
        assertThat(statusDetails.get(1).getMessage(), is("some message"));
    }

}