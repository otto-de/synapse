package de.otto.synapse.state;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.openhft.chronicle.map.ChronicleMapBuilder;

public class ChronicleMapStateRepository<V> extends StateRepository<V> {
    private static final int DEFAULT_KEY_SIZE_BYTES = 128;
    private static final double DEFAULT_VALUE_SIZE_BYTES = 512;
    private static final long DEFAULT_ENTRY_COUNT = 1_000_00;

    private final Class<V> clazz;
    private final ObjectMapper objectMapper;

    private ChronicleMapStateRepository(Builder builder) {
        super(builder.chronicleMapBuilder.create());
        clazz = builder.clazz;
        objectMapper = builder.objectMapper;
    }

    public static <V> Builder chronicleMapConcurrentMapStateRepositoryBuilder(Class<V> clazz) {
        return new Builder(clazz);
    }

    public static final class Builder<V> {

        private ObjectMapper objectMapper;

        private final Class<V> clazz;
        private ChronicleMapBuilder<String, V> chronicleMapBuilder;

        private Builder(Class<V> clazz) {
            this.clazz = clazz;
        }

        public Builder<V> withObjectMapper(ObjectMapper val) {
            objectMapper = val;
            return this;
        }

        public Builder<V> withMapBuilder(ChronicleMapBuilder<String, V> val) {
            chronicleMapBuilder = val;
            return this;
        }

        public ChronicleMapStateRepository build() {
            if (objectMapper == null)  {
                objectMapper = new ObjectMapper();
            }

            if (chronicleMapBuilder == null) {
                chronicleMapBuilder = ChronicleMapBuilder.of(String.class, clazz)
                        .averageKeySize(DEFAULT_KEY_SIZE_BYTES)
                        .averageValueSize(DEFAULT_VALUE_SIZE_BYTES)
                        .entries(DEFAULT_ENTRY_COUNT);
            }

            chronicleMapBuilder.valueMarshaller(new ChronicleMapBytesMarshaller<>(objectMapper, clazz));

            return new ChronicleMapStateRepository(this);
        }
    }
}
