package de.otto.synapse.edison.state;

import de.otto.edison.hal.HalRepresentation;
import de.otto.edison.hal.Links;

public class EntityHalRepresentation extends HalRepresentation {

    private final Object entity;

    public EntityHalRepresentation(final Links links,
                                   final Object entity) {
        super(links);
        this.entity = entity;
    }

    public Object getEntity() {
        return entity;
    }
}
