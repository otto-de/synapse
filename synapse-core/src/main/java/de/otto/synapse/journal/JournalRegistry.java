package de.otto.synapse.journal;

import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import org.slf4j.Logger;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static de.otto.synapse.endpoint.MessageInterceptorRegistration.matchingReceiverChannelsWith;
import static java.util.Collections.newSetFromMap;
import static java.util.Optional.ofNullable;
import static org.slf4j.LoggerFactory.getLogger;
import static org.springframework.core.Ordered.LOWEST_PRECEDENCE;

public class JournalRegistry {
    private static final Logger LOG = getLogger(JournalRegistry.class);
    private final ConcurrentMap<String,Journal> journals;
    private final Set<String> registeredInterceptors = newSetFromMap(new ConcurrentHashMap<>());
    private final MessageInterceptorRegistry registry;

    public JournalRegistry(final List<Journal> journals,
                           final MessageInterceptorRegistry registry) {
        this.registry = registry;
        this.journals = new ConcurrentHashMap<>();
        journals.forEach(this::add);
    }

    public Optional<Journal> getJournal(final String name) {
        return ofNullable(
                journals.get(name)
        );
    }

    private void add(final Journal journal) {
        final Journal existing = journals.putIfAbsent(journal.getName(), journal);
        if (existing != null) {
            throw new IllegalStateException("Unable to register Journal " + journal.getName() + " as there is already a different journal registered for this name");
        } else {
            journal.getJournaledChannels().forEach(channelName -> {
                if (!registeredInterceptors.contains(channelName + journal.getName())) {
                    final JournalingInterceptor journalingInterceptor = new JournalingInterceptor(channelName, journal);
                    registry.register(matchingReceiverChannelsWith(channelName, journalingInterceptor, LOWEST_PRECEDENCE));
                    registeredInterceptors.add(channelName + journal.getName());
                }
            });

        }
    }

    public boolean hasJournal(final String journalName) {
        return journals.containsKey(journalName);
    }
}
