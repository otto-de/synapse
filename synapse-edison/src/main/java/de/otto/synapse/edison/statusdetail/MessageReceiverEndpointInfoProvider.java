package de.otto.synapse.edison.statusdetail;

import de.otto.synapse.channel.ChannelDurationBehind;
import de.otto.synapse.eventsource.EventSource;
import de.otto.synapse.info.MessageReceiverEndpointInfo;
import de.otto.synapse.info.MessageReceiverEndpointInfos;
import de.otto.synapse.info.MessageReceiverNotification;
import de.otto.synapse.info.MessageReceiverStatus;
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

import static de.otto.synapse.channel.ChannelDurationBehind.unknown;
import static java.lang.String.format;

@Component
public class MessageReceiverEndpointInfoProvider {


    private final MessageReceiverEndpointInfos messageReceiverEndpointInfos = new MessageReceiverEndpointInfos();

    private final Set<String> allChannels = new ConcurrentSkipListSet<>();
    private final Set<String> startedChannels = new ConcurrentSkipListSet<>();

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
    public void on(final MessageReceiverNotification notification) {
        String channelName = notification.getChannelName();
        switch (notification.getStatus()) {
            case STARTING:
                final MessageReceiverEndpointInfo.Builder builder = MessageReceiverEndpointInfo
                        .builder()
                        .withChannelName(notification.getChannelName())
                        .withStatus(MessageReceiverStatus.STARTING)
                        .withMessage(notification.getMessage());
                messageReceiverEndpointInfos.update(channelName, builder.build());
                channelStartupTimes.put(channelName, clock.instant());
                break;
            case STARTED:
                messageReceiverEndpointInfos.update(channelName, MessageReceiverEndpointInfo
                        .builder()
                        .withChannelName(channelName)
                        .withStatus(notification.getStatus())
                        .withMessage(notification.getMessage())
                        .build());
                break;
            case RUNNING:
                final MessageReceiverEndpointInfo endpointInfo = MessageReceiverEndpointInfo
                        .builder()
                        .withChannelName(channelName)
                        .withStatus(notification.getStatus())
                        .withChannelDurationBehind(notification.getChannelDurationBehind().orElse(unknown()))
                        .withMessage(format("Channel is %s behind head.", notification.getChannelDurationBehind()))
                        .build();
                messageReceiverEndpointInfos.update(channelName, endpointInfo);
                break;
            case FINISHED:
                final Duration runtime = Duration.between(channelStartupTimes.get(channelName), clock.instant());
                ChannelDurationBehind lastDurationBehind = messageReceiverEndpointInfos.getChannelInfoFor(channelName).getDurationBehind().orElse(null);
                messageReceiverEndpointInfos.update(channelName, MessageReceiverEndpointInfo
                        .builder()
                        .withChannelName(channelName)
                        .withStatus(notification.getStatus())
                        .withChannelDurationBehind(lastDurationBehind)
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
            default:
                break;
        }
    }

    public MessageReceiverEndpointInfos getInfos() {
        return messageReceiverEndpointInfos;
    }

}
