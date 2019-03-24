package de.otto.synapse.state;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.openhft.chronicle.hash.ChronicleHashClosedException;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import static de.otto.synapse.translator.ObjectMappers.currentObjectMapper;

public class ChronicleMapStateRepository<V> extends ConcurrentMapStateRepository<V> {

    private static final Logger LOG = LoggerFactory.getLogger(ChronicleMapStateRepository.class);

    private static final int DEFAULT_KEY_SIZE_BYTES = 128;
    private static final double DEFAULT_VALUE_SIZE_BYTES = 512;
    private static final long DEFAULT_ENTRY_COUNT = 1_000_00;

    private ChronicleMapStateRepository(final String name,
                                        final ChronicleMap<String, V> chronicleMap) {
        super(name, chronicleMap);
    }

    @Override
    public Optional<V> put(String key, V value) {
        try {
            return super.put(key, value);
        } catch (ChronicleHashClosedException e) {
            LOG.warn("could not put on closed state repository", e);
            return Optional.empty();
        }
    }

    @Override
    public Optional<V> get(String key) {
        try {
            return super.get(key);
        } catch (ChronicleHashClosedException e) {
            LOG.warn("could not get on closed state repository", e);
            return Optional.empty();
        }
    }

    @Override
    public long size() {
        try {
            return super.size();
        } catch (ChronicleHashClosedException e) {
            LOG.warn("could not get size on closed state repository", e);
            return 0;
        }
    }

    public static <V> Builder<V> builder(Class<V> clazz) {
        return new Builder<>(clazz);
    }

    public static final class Builder<V> {

        private ObjectMapper objectMapper = currentObjectMapper();

        private final Class<V> clazz;
        private String name;
        private ChronicleMapBuilder<String, V> chronicleMapBuilder;

        private Builder(Class<V> clazz) {
            this.clazz = clazz;
        }

        public Builder<V> withObjectMapper(ObjectMapper val) {
            objectMapper = val;
            return this;
        }

        public Builder<V> withName(final String val) {
            name = val;
            return this;
        }

        public Builder<V> withMapBuilder(ChronicleMapBuilder<String, V> val) {
            chronicleMapBuilder = val;
            return this;
        }

        public ChronicleMapStateRepository<V> build() {

            if (chronicleMapBuilder == null) {
                chronicleMapBuilder = ChronicleMapBuilder.of(String.class, clazz)
                        .averageKeySize(DEFAULT_KEY_SIZE_BYTES)
                        .averageValueSize(DEFAULT_VALUE_SIZE_BYTES)
                        .entries(DEFAULT_ENTRY_COUNT);
            }

            boolean doesClassNeedToBeSerialized = clazz != String.class;
            if (doesClassNeedToBeSerialized) {
                chronicleMapBuilder.valueMarshaller(new ChronicleMapBytesMarshaller<>(objectMapper, clazz));
            }

            return new ChronicleMapStateRepository<>(name, chronicleMapBuilder.create());
        }
    }
}
