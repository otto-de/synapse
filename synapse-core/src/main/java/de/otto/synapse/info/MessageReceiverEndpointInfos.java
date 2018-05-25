package de.otto.synapse.info;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import static de.otto.synapse.info.MessageReceiverEndpointInfo.builder;
import static java.util.Comparator.comparing;

public class MessageReceiverEndpointInfos {

    private static final String MSG_CHANNEL_STARTING = "Channel is starting";

    private Map<String, MessageReceiverEndpointInfo> channelNameToEndpointInfo = new ConcurrentHashMap<>();


    public void add(final String channelName) {
        channelNameToEndpointInfo.put(channelName, builder()
                .withChannelName(channelName)
                .withMessage(MSG_CHANNEL_STARTING)
                .withStatus(MessageReceiverStatus.STARTING)
                .build());
    }

    public void update(final String channelName,
                       final MessageReceiverEndpointInfo info) {
        channelNameToEndpointInfo.put(channelName, info);
    }

    public Stream<MessageReceiverEndpointInfo> stream() {
        return channelNameToEndpointInfo.values().stream().sorted(comparing(MessageReceiverEndpointInfo::getChannelName));
    }

    public MessageReceiverEndpointInfo getChannelInfoFor(final String channel) {
        if (!channelNameToEndpointInfo.containsKey(channel)) {
            throw new IllegalArgumentException("Unknown channel '" + channel + "'");
        }
        return channelNameToEndpointInfo.get(channel);
    }
}
