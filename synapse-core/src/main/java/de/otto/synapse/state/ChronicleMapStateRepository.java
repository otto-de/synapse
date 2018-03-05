package de.otto.synapse.state;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;

public class ChronicleMapStateRepository<V> extends StateRepository<V> {
    private static final int DEFAULT_KEY_SIZE_BYTES = 128;
    private static final double DEFAULT_VALUE_SIZE_BYTES = 512;
    private static final long DEFAULT_ENTRY_COUNT = 1_000_00;

    private final Class<V> clazz;
    private final ObjectMapper objectMapper;

    private ChronicleMapStateRepository(Builder builder) {
        super(builder.concurrentMap);
        clazz = builder.clazz;
        objectMapper = builder.objectMapper;
    }

    public static <V> Builder chronicleMapConcurrentMapStateRepositoryBuilder(Class<V> clazz) {
        return new Builder(clazz);
    }

    public static final class Builder<V> {

        private final static ObjectMapper objectMapper = new ObjectMapper();

        private final Class<V> clazz;
        private ChronicleMap<String, V> concurrentMap;

        private Builder(Class<V> clazz) {
            this.clazz = clazz;
        }

        public Builder<V> withStore(ChronicleMap<String, V> val) {
            concurrentMap = val;
            return this;
        }

        public ChronicleMapStateRepository build() {
            if (concurrentMap == null) {
                concurrentMap = ChronicleMapBuilder.of(String.class, clazz)
                        .valueMarshaller(new ChronicleMapBytesMarshaller<>(objectMapper, clazz))
                        .averageKeySize(DEFAULT_KEY_SIZE_BYTES)
                        .averageValueSize(DEFAULT_VALUE_SIZE_BYTES)
                        .entries(DEFAULT_ENTRY_COUNT)
                        .create();
            }
            return new ChronicleMapStateRepository(this);
        }
    }
}
