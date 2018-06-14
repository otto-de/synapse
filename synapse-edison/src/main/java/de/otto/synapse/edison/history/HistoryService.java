package de.otto.synapse.edison.history;

import static de.otto.synapse.message.Message.message;
import static java.util.Arrays.asList;

public class HistoryService {

    /**
     * Returns the {@link History} of an entity.
     *
     *
     * @param type selects the {@link de.otto.synapse.state.StateRepository} that is holding the entity
     * @param entityId the entity
     * @return History
     */
    public History getHistory(final String type,
                              final String entityId) {
        final History history = new History(
                entityId,
                asList(
                        new HistoryEntry(
                                message("4711", "{\"price\":45}"),
                                "test-products",
                                asList(
                                        new Diff("price", 46, 45))
                        ),
                        new HistoryEntry(
                                message("4711", "{\"price\":42}"),
                                "test-products",
                                asList(
                                        new Diff("price", 45, 42))
                        )
                ));
        return history;
    }

}
