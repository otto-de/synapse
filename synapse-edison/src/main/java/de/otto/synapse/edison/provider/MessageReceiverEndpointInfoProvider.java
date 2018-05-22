package de.otto.synapse.edison.provider;

import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.eventsource.EventSource;
import de.otto.synapse.info.MessageEndpointNotification;
import de.otto.synapse.info.MessageEndpointStatus;
import de.otto.synapse.info.MessageReceiverEndpointInfo;
import de.otto.synapse.info.MessageReceiverEndpointInfos;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static de.otto.synapse.channel.ChannelPosition.fromHorizon;
import static java.lang.String.format;

@Component
public class MessageReceiverEndpointInfoProvider {


    private final MessageReceiverEndpointInfos messageReceiverEndpointInfos = new MessageReceiverEndpointInfos();

    private final Map<String, ChannelPosition> mapChannelToDurationBehind = new ConcurrentHashMap<>();
    private final Map<String, Instant> channelStartupTimes = new ConcurrentHashMap<>();
    private final Clock clock;

    @Autowired
    public MessageReceiverEndpointInfoProvider(final Optional<List<EventSource>> eventSources) {
        this(eventSources, Clock.systemDefaultZone());
    }

    /**
     * For testing purposes only.
     *
     * @param eventSources the event sources
     * @param clock clock used to time events end testing the timing behaviour of the provider
     */
    public MessageReceiverEndpointInfoProvider(final Optional<List<EventSource>> eventSources, final Clock clock) {
        eventSources.ifPresent(es -> {
            es.stream()
                    .map(EventSource::getChannelName)
                    .forEach(messageReceiverEndpointInfos::add);
        });
        this.clock = clock;
    }

    @EventListener
    public void onEventSourceNotification(MessageEndpointNotification notification) {
        String channelName = notification.getChannelName();

        switch (notification.getStatus()) {
            case STARTING:
                final MessageReceiverEndpointInfo.Builder builder = MessageReceiverEndpointInfo
                        .builder()
                        .withChannelName(notification.getChannelName())
                        .withStatus(MessageEndpointStatus.STARTING)
                        .withMessage(notification.getMessage());
                messageReceiverEndpointInfos.update(channelName, builder.build());
                channelStartupTimes.put(channelName, clock.instant());
                break;
            case STARTED:
                // TODO ALl shards known, set aggregated duration behind from all shards as MessageReceiverEndpointInfo.durationBehind
                messageReceiverEndpointInfos.update(channelName, MessageReceiverEndpointInfo
                        .builder()
                        .withChannelName(channelName)
                        .withStatus(notification.getStatus())
                        .withChannelPosition(notification.getChannelPosition())
                        .withMessage(notification.getMessage())
                        .build());
                break;
            case RUNNING:
                final ChannelPosition channelPosition = notification.getChannelPosition() != null
                        ? notification.getChannelPosition()
                        : fromHorizon();
                if (!channelPosition.shards().isEmpty()) {
                    ChannelPosition previousChannelPosition = mapChannelToDurationBehind.getOrDefault(channelName, fromHorizon());
                    ChannelPosition mergedChannelPosition = ChannelPosition.merge(previousChannelPosition, channelPosition);
                    mapChannelToDurationBehind.put(channelName, mergedChannelPosition);
                    messageReceiverEndpointInfos.update(channelName, MessageReceiverEndpointInfo
                            .builder()
                            .withChannelName(channelName)
                            .withStatus(notification.getStatus())
                            .withChannelPosition(mergedChannelPosition)
                            .withMessage(format("Channel is %s behind head.", mergedChannelPosition.getDurationBehind()))
                            .build());
                } else {
                    messageReceiverEndpointInfos.update(channelName, MessageReceiverEndpointInfo
                            .builder()
                            .withChannelName(channelName)
                            .withStatus(notification.getStatus())
                            .withMessage("Unknown duration behind head")
                            .build());
                }
                break;
            case FINISHED:
                final Duration runtime = Duration.between(channelStartupTimes.get(channelName), clock.instant());
                messageReceiverEndpointInfos.update(channelName, MessageReceiverEndpointInfo
                        .builder()
                        .withChannelName(channelName)
                        .withStatus(notification.getStatus())
                        .withMessage(format("%s Finished consumption after %s.", notification.getMessage(), runtime))
                        .build());
                break;
            case FAILED:
                messageReceiverEndpointInfos.update(channelName, MessageReceiverEndpointInfo
                        .builder()
                        .withChannelName(channelName)
                        .withStatus(notification.getStatus())
                        .withMessage(notification.getMessage())
                        .build());
                break;
        }

    }

    public MessageReceiverEndpointInfos getInfos() {
        return messageReceiverEndpointInfos;
    }

}
