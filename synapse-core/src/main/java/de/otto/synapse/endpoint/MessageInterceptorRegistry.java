package de.otto.synapse.endpoint;

import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Collections.synchronizedList;

public class MessageInterceptorRegistry {

    private final List<MessageInterceptorRegistration> registry = synchronizedList(new ArrayList<>());
    private final ConcurrentMap<String, InterceptorChain> interceptorChainCache = new ConcurrentHashMap<>();

    public void register(final @Nonnull MessageInterceptorRegistration registration) {
        registry.add(registration);
        interceptorChainCache.clear();
    }

    @Nonnull
    public InterceptorChain getInterceptorChain(final String channelName,
                                                final EndpointType endpointType) {
        return interceptorChainCache.computeIfAbsent(channelName + "#" + endpointType.name(), (key) -> {
            final InterceptorChain interceptorChain = new InterceptorChain();
            getRegistrations(channelName, endpointType)
                    .forEach(registration -> interceptorChain.register(registration.getInterceptor()));
            return interceptorChain;
        });
    }

    @Nonnull
    public ImmutableList<MessageInterceptorRegistration> getRegistrations(final String channelName,
                                                                          final EndpointType endpointType) {
        return registry
                .stream()
                .filter(registration -> registration.isEnabledFor(channelName, endpointType))
                .collect(toImmutableList());
    }

}
