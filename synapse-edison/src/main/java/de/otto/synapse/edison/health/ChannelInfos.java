package de.otto.synapse.edison.health;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static de.otto.synapse.edison.health.ChannelInfo.Status.BEHIND;
import static de.otto.synapse.edison.health.ChannelInfo.Status.HEAD;
import static de.otto.synapse.edison.health.ChannelInfo.startupDetailBuilder;

public class ChannelInfos {

    private static final String CHANNEL_NOT_YET_FINISHED = "Channel not yet finished";
    private static final String CHANNEL_AT_HEAD = "Channel at HEAD position";

    private Map<String, ChannelInfo> mapChannelToStartupDetail = new ConcurrentHashMap<>();


    public void addChannel(final String channel) {
        mapChannelToStartupDetail.put(channel, startupDetailBuilder()
                .withMessage(CHANNEL_NOT_YET_FINISHED)
                .withStatus(BEHIND)
                .build());
    }

    public void markHeadPosition(final String channel) {
        mapChannelToStartupDetail.put(channel, startupDetailBuilder()
                .withMessage(CHANNEL_AT_HEAD)
                .withStatus(HEAD)
                .build());
    }

    public boolean isAtHead() {
        return mapChannelToStartupDetail.isEmpty() || mapChannelToStartupDetail.values().stream().allMatch(detail -> detail.getStatus() == HEAD);
    }

    public Set<String> getChannels() {
        return mapChannelToStartupDetail.keySet();
    }

    public ChannelInfo getStartupInfo(final String channel) {
        if (!mapChannelToStartupDetail.containsKey(channel)) {
            throw new IllegalArgumentException("Unknown channel '" + channel + "'");
        }
        return mapChannelToStartupDetail.get(channel);
    }
}
