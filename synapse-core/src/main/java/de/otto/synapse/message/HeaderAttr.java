package de.otto.synapse.message;

/**
 * Interface used to mark enums as keys for header-attributes.
 */
public interface HeaderAttr {
    String name();

    default String key() {
        return this.name();
    }
}
