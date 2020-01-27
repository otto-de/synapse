package de.otto.synapse.edison.health;

import java.util.HashSet;
import java.util.List;


public class FixedChannelHealthIndicatorTest extends AbstractChannelHealthIndicatorTest{


    @Override
    protected AbstractChannelHealthIndicator createHealthIndicator(List<String> channelNames) {
        return new FixedChannelHealthIndicator(new HashSet<>(channelNames));
    }
}
