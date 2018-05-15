package de.otto.synapse.endpoint;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.synchronizedList;
import static java.util.stream.Collectors.toList;

public class MessageInterceptorRegistry {

    private final List<MessageInterceptorRegistration> registry = synchronizedList(new ArrayList<>());

    public void register(final @Nonnull MessageInterceptorRegistration registration) {
        registry.add(registration);
    }

    @Nonnull
    public List<MessageInterceptorRegistration> getRegistrations(final String channelName,
                                                                 final EndpointType endpointType) {
        return registry
                .stream()
                .filter(registration -> registration.isEnabledFor(channelName, endpointType))
                .collect(toList());
    }

}
