package de.otto.synapse.aws.s3;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.info.MessageEndpointNotification;
import de.otto.synapse.eventsource.aws.SnapshotMessageEndpointNotification;
import de.otto.synapse.info.MessageEndpointStatus;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.context.ApplicationEventPublisher;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.File;
import java.util.Optional;

import static de.otto.synapse.channel.ChannelPosition.fromHorizon;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class SnapshotEventSourceTest {

    private static final String STREAM_NAME = "test-stream";

    @Mock
    private SnapshotReadService snapshotReadService;

    @Mock
    private SnapshotConsumerService snapshotConsumerService;

    @Mock
    private ApplicationEventPublisher applicationEventPublisher;

    private SnapshotEventSource snapshotEventSource;


    @Before
    public void init() {
        snapshotEventSource = new SnapshotEventSource(
                "snapshotEventSource",
                STREAM_NAME,
                snapshotReadService,
                snapshotConsumerService,
                applicationEventPublisher,
                new ObjectMapper());
        snapshotEventSource.register(MessageConsumer.of(".*", String.class, (event)->{}));
    }

    @Test(expected = RuntimeException.class)
    public void shouldThrowExceptionIfDownloadFails() {
        // given
        when(snapshotReadService.retrieveLatestSnapshot(any())).thenReturn(Optional.of(new File("someFileWithPattern-snapshot-2018-01-01T00-00Z-1234567890123456789.json.zip")));
        when(snapshotConsumerService.consumeSnapshot(any(),any(),any())).thenThrow(new RuntimeException("boom - simulate exception while loading from S3"));

        // when
        snapshotEventSource.consume();

        // then expect exception
    }

    @Test
    public void shouldThrowExceptionIfBucketNotExists() {
        // given
        S3Exception bucketNotFoundException = new S3Exception("boom - simulate exception while loading from S3");
        when(snapshotReadService.retrieveLatestSnapshot(any())).thenThrow(bucketNotFoundException);

        // when
        try {
            snapshotEventSource.consume();

            fail("should throw RuntimeException");
        } catch (RuntimeException ignored) {

        }

        // then
        MessageEndpointNotification expectedFailedEvent = SnapshotMessageEndpointNotification.builder()
                .withEventSourceName("snapshotEventSource")
                .withChannelName(STREAM_NAME)
                .withStatus(MessageEndpointStatus.FAILED)
                .withChannelPosition(fromHorizon())
                .withMessage("Failed to load snapshot from S3: boom - simulate exception while loading from S3 (Service: null; Status Code: 0; Request ID: null)")
                .build();

        ArgumentCaptor<MessageEndpointNotification> notificationArgumentCaptor = ArgumentCaptor.forClass(MessageEndpointNotification.class);
        verify(applicationEventPublisher, times(2)).publishEvent(notificationArgumentCaptor.capture());

        assertThat(notificationArgumentCaptor.getAllValues().get(1), is(expectedFailedEvent));
    }

    @Test
    public void shouldDeleteOlderSnapshotsInCaseOfAnException() {
        // given
        S3Exception bucketNotFoundException = new S3Exception("boom - simulate exception while loading from S3");
        when(snapshotReadService.retrieveLatestSnapshot(any())).thenThrow(bucketNotFoundException);

        // when
        try {
            snapshotEventSource.consume();
        } catch (Exception e) {
            // ignore exception
        }

        // then
        verify(snapshotReadService).deleteOlderSnapshots(STREAM_NAME);
    }

    @Test
    public void shouldPublishStartAndFinishEvents() {
        // given
        when(snapshotReadService.retrieveLatestSnapshot(any())).thenReturn(Optional.empty());

        // when
        snapshotEventSource.consume();

        // then
        MessageEndpointNotification expectedStartEvent = SnapshotMessageEndpointNotification.builder()
                .withEventSourceName("snapshotEventSource")
                .withChannelName(STREAM_NAME)
                .withStatus(MessageEndpointStatus.STARTING)
                .withMessage("Loading snapshot from S3.")
                .withChannelPosition(fromHorizon())
                .build();
        verify(applicationEventPublisher).publishEvent(expectedStartEvent);

        MessageEndpointNotification expectedFinishedEvent = SnapshotMessageEndpointNotification.builder()
                .withEventSourceName("snapshotEventSource")
                .withChannelName(STREAM_NAME)
                .withStatus(MessageEndpointStatus.FINISHED)
                .withMessage("Finished to load snapshot from S3.")
                .withChannelPosition(fromHorizon())
                .build();
        verify(applicationEventPublisher).publishEvent(expectedFinishedEvent);
    }
}
