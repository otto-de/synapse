package de.otto.synapse.edison.health;

import de.otto.synapse.endpoint.MessageInterceptor;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.eventsource.EventSource;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;

import static de.otto.synapse.endpoint.MessageInterceptorRegistration.matchingReceiverChannelsWith;

@Component
public class ChannelInfoProvider {


    private final ChannelInfos startupDetails = new ChannelInfos();

    public ChannelInfoProvider(final Optional<List<EventSource>> eventSources,
                               final MessageInterceptorRegistry registry) {
        eventSources.ifPresent(es -> {
            es.stream().map(EventSource::getChannelName)
                    .forEach(channel -> {
                        startupDetails.addChannel(channel);
                        registry.register(matchingReceiverChannelsWith(channel, durationBehindInterceptor(channel)));
                    });
        });
    }

    private MessageInterceptor durationBehindInterceptor(final String channel) {
        return (message) -> {
            message.getHeader().getDurationBehind().ifPresent((durationBehind) -> {
                if (durationBehind.isZero()) {
                    startupDetails.markHeadPosition(channel);
                }
            });
            return message;
        };
    }

    public ChannelInfos getInfos() {
        return startupDetails;
    }

}
