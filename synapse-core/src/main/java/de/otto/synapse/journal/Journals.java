package de.otto.synapse.journal;

import org.slf4j.Logger;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.util.Optional.ofNullable;
import static org.slf4j.LoggerFactory.getLogger;

public class Journals {
    private static final Logger LOG = getLogger(Journals.class);
    private final ConcurrentMap<String,Journal> journals;

    public Journals() {
        journals = new ConcurrentHashMap<>();
    }

    public Optional<Journal> getJournal(final String name) {
        return ofNullable(
                journals.get(name)
        );
    }

    public void add(final Journal journal) {
        final Journal existing = journals.putIfAbsent(journal.getName(), journal);
        if (existing != null && existing != journal) {
            throw new IllegalStateException("Unable to register Journal " + journal.getName() + " as there is already a different journal registered for this name");
        }
    }

    public boolean containsKey(final String repositoryName) {
        return journals.containsKey(repositoryName);
    }
}
