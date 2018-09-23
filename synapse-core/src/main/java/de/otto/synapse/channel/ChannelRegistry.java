package de.otto.synapse.channel;

import org.slf4j.Logger;

import javax.annotation.Nullable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.lang.String.format;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * A registry used to store the channels available for the application.
 */
public class ChannelRegistry {

    private static final Logger LOG = getLogger(ChannelRegistry.class);

    private final ConcurrentMap<String, ChannelType> registeredChannels = new ConcurrentHashMap<>();

    public void register(final String channelName, final ChannelType type) {
        registeredChannels.compute(channelName, (channelName1, previousType) -> {
            final String check = previousType != null ? previousType.name() : type.name();
            if (check.equals(type.name())) {
                LOG.info("Registered channelName {} with type {}", channelName, type);
                return type;
            } else {
                throw new IllegalArgumentException(format("Unable to register the same channel with name=%s type=%s with a different channelType. Previous type was %s", channelName1, type, previousType));
            }
        });
    }

    public @Nullable ChannelType get(final String channelName) {
        return registeredChannels.get(channelName);
    }
}
