package de.otto.synapse.state.concurrent;

import net.openhft.chronicle.map.ChronicleMapBuilder;

public class ChronicleMapConcurrentMapStateRepository<V> extends ConcurrentMapStateRepository<V> {
    private static final int DEFAULT_KEY_SIZE_BYTES = 128;
    private static final double DEFAULT_VALUE_SIZE_BYTES = 512;
    private static final long DEFAULT_ENTRY_COUNT = 1_000_00;

    public ChronicleMapConcurrentMapStateRepository(Class<V> clazz) {
        super(ChronicleMapBuilder.of(String.class, clazz)
                .averageKeySize(DEFAULT_KEY_SIZE_BYTES)
                .averageValueSize(DEFAULT_VALUE_SIZE_BYTES)
                .entries(DEFAULT_ENTRY_COUNT)
                .create());
    }
}
