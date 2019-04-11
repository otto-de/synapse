package de.otto.synapse.edison.journal;

import de.otto.edison.hal.HalRepresentation;
import de.otto.edison.hal.Links;

import java.util.List;

public class JournalHalRepresentation extends HalRepresentation {

    private final List<MessageStoreEntryRepresentation> messages;

    public JournalHalRepresentation(final Links links,
                                    final List<MessageStoreEntryRepresentation> entries) {
        super(links);
        this.messages = entries;
    }

    public List<MessageStoreEntryRepresentation> getMessages() {
        return messages;
    }
}
