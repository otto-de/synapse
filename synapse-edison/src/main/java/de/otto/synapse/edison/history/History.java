package de.otto.synapse.edison.history;

import de.otto.synapse.message.Message;

import java.util.List;
import java.util.Objects;

/**
 * The history of a single entity that is aggregated from {@link Message messages} and stored in a
 * {@link de.otto.synapse.state.StateRepository}.
 * <p>
 *     A {@code History} consists of an {@code entityId}, and a list of {@@link HistoryEntry history entries}.
 * </p>
 * <p>
 *     The {@code entityId} corresponds to the {@code key} of an entry in the {@code StateRepository} that is used
 *     to aggregate messages. Multiple messages from one or more channels may be aggregated into a single entity, so
 *     the history entries contain not only the message, but also the the name of the channel that was the origin of
 *     the message.
 * </p>
 */
public class History {

    private final String entityId;
    private final List<HistoryEntry> entries;

    public History(final String entityId,
                   final List<HistoryEntry> entries) {
        this.entityId = entityId;
        this.entries = entries;
    }

    public String getEntityId() {
        return entityId;
    }

    public List<HistoryEntry> getEntries() {
        return entries;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        History history = (History) o;
        return Objects.equals(entityId, history.entityId) &&
                Objects.equals(entries, history.entries);
    }

    @Override
    public int hashCode() {

        return Objects.hash(entityId, entries);
    }

    @Override
    public String toString() {
        return "History{" +
                "entityId='" + entityId + '\'' +
                ", entries=" + entries +
                '}';
    }
}
