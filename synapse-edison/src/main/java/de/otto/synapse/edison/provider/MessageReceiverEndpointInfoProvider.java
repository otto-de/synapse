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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

import static de.otto.synapse.channel.ChannelPosition.fromHorizon;
import static de.otto.synapse.channel.ChannelPosition.merge;
import static java.lang.String.format;

@Component
public class MessageReceiverEndpointInfoProvider {


    private static final long MAX_SECONDS_BEHIND = 10;
    private final MessageReceiverEndpointInfos messageReceiverEndpointInfos = new MessageReceiverEndpointInfos();

    private final Set<String> allChannels = new ConcurrentSkipListSet<>();
    private final Set<String> startedChannels = new ConcurrentSkipListSet<>();

    private final Map<String, ChannelPosition> mapChannelToPosition = new ConcurrentHashMap<>();
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
                    .forEach( channelName -> {
                        allChannels.add(channelName);
                        messageReceiverEndpointInfos.add(channelName);
                    });
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
                        .withChannelPosition(notification.getChannelPosition())
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
                mapChannelToPosition.put(channelName, notification.getChannelPosition());
                break;
            case RUNNING:
                final ChannelPosition channelPosition = notification.getChannelPosition() != null
                        ? notification.getChannelPosition()
                        : fromHorizon();
                ChannelPosition previousChannelPosition = mapChannelToPosition.getOrDefault(channelName, fromHorizon());
                ChannelPosition mergedChannelPosition = merge(previousChannelPosition, channelPosition);
                mapChannelToPosition.put(channelName, mergedChannelPosition);
                final MessageReceiverEndpointInfo endpointInfo = MessageReceiverEndpointInfo
                        .builder()
                        .withChannelName(channelName)
                        .withStatus(notification.getStatus())
                        .withChannelPosition(mergedChannelPosition)
                        .withMessage(format("Channel is %s behind head.", mergedChannelPosition.getDurationBehind()))
                        .build();
                if (isCurrentlyUpToDate(endpointInfo)) {
                    startedChannels.add(channelName);
                }
                messageReceiverEndpointInfos.update(channelName, endpointInfo);
                break;
            case FINISHED:
                final Duration runtime = Duration.between(channelStartupTimes.get(channelName), clock.instant());
                messageReceiverEndpointInfos.update(channelName, MessageReceiverEndpointInfo
                        .builder()
                        .withChannelName(channelName)
                        .withStatus(notification.getStatus())
                        .withChannelPosition(mapChannelToPosition.get(channelName))
                        .withMessage(format("%s Finished consumption after %s.", notification.getMessage(), runtime))
                        .build());
                break;
            case FAILED:
                messageReceiverEndpointInfos.update(channelName, MessageReceiverEndpointInfo
                        .builder()
                        .withChannelName(channelName)
                        .withStatus(notification.getStatus())
                        .withMessage(notification.getMessage())
                        .withChannelPosition(mapChannelToPosition.get(channelName))
                        .build());
                break;
            default:
                break;
        }

    }

    public MessageReceiverEndpointInfos getInfos() {
        return messageReceiverEndpointInfos;
    }

    public boolean allChannelsStarted() {
        return startedChannels.containsAll(allChannels);
    }

    private boolean isCurrentlyUpToDate(MessageReceiverEndpointInfo info) {
        return info.getStatus() != MessageEndpointStatus.STARTING && info.getChannelPosition().get()
                .getDurationBehind()
                .minusSeconds(MAX_SECONDS_BEHIND)
                .isNegative();
    }

}
