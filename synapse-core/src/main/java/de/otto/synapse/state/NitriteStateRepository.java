package de.otto.synapse.state;

import com.fasterxml.jackson.core.type.TypeReference;
import org.dizitart.no2.*;

import java.io.Closeable;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.collect.Sets.newHashSet;
import static de.otto.synapse.translator.ObjectMappers.currentObjectMapper;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.StreamSupport.stream;
import static org.dizitart.no2.Document.createDocument;
import static org.dizitart.no2.IndexOptions.indexOptions;
import static org.dizitart.no2.IndexType.NonUnique;
import static org.dizitart.no2.IndexType.Unique;
import static org.dizitart.no2.filters.Filters.ALL;
import static org.dizitart.no2.filters.Filters.eq;

public class NitriteStateRepository<V> implements StateRepository<V>, Closeable {

    private static final String IDX_ID = "_idx_id";
    private static final TypeReference<Map<String,Object>> JSON_MAP = new TypeReference<Map<String, Object>>() {};
    private static final Document ID_PROJECTION = createDocument(IDX_ID, null);

    private final String name;
    private final Class<V> valueType;
    private final Nitrite nitrite;
    private final NitriteCollection collection;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public NitriteStateRepository(final String name,
                                  final Class<V> valueType,
                                  final Set<String> indexedFields,
                                  final NitriteBuilder builder) {
        this.name = name;
        this.valueType = valueType;
        this.nitrite = builder.openOrCreate();
        this.collection = nitrite.getCollection(name);
        this.collection.createIndex(IDX_ID, indexOptions(Unique));

        indexedFields.forEach(field -> collection.createIndex(field, indexOptions(NonUnique)));
    }

    public NitriteStateRepository(final String name,
                                  final Class<V> valueType,
                                  final Set<String> indexedFields) {
        this(name, valueType, indexedFields, Nitrite.builder().compressed());
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Set<String> keySet() {
        try {
            lock.readLock().lock();
            return stream(collection
                    .find()
                    .project(ID_PROJECTION)
                    .spliterator(), false)
                    .map(d -> d.getOrDefault(IDX_ID, "").toString())
                    .collect(toSet());
        } finally {
            lock.readLock().unlock();
        }
    }

    public Collection<V> findBy(final Filter filter) {
        return findInternal(() -> collection.find(filter));
    }

    public Collection<V> findBy(final FindOptions findOptions) {
        return findInternal(() -> collection.find(findOptions));
    }

    public Collection<V> findBy(final Filter filter, final FindOptions findOptions) {
        return findInternal(() -> collection.find(filter, findOptions));
    }

    public Collection<V> findBy(final String key, final Object value) {
        return findInternal(() -> collection.find(eq(key, value)));
    }

    private Collection<V> findInternal(final Supplier<Cursor> findFunc) {
        try {
            lock.readLock().lock();
            final Cursor documents = findFunc.get();
            return stream(documents.spliterator(), false)
                    .map(document -> currentObjectMapper().convertValue(document, valueType))
                    .collect(toList());
        } finally {
            lock.readLock().unlock();
        }
    }
    @Override
    public Optional<V> get(String key) {
        try {
            lock.readLock().lock();
            final Document document = collection
                .find(eq(IDX_ID, key))
                .firstOrDefault();
            return document != null
                    ? of(currentObjectMapper().convertValue(document, valueType))
                    : empty();
        } finally {
            lock.readLock().unlock();
        }

    }

    @Override
    public void consumeAll(final BiConsumer<? super String, ? super V> consumer) {
        try {
            lock.readLock().lock();
            collection.find().forEach(document -> {
                final String key = document.get(IDX_ID).toString();
                final V value = currentObjectMapper().convertValue(document, valueType);
                if (key == null) {
                    throw new IllegalStateException("Unexpected null value found for required field '" + IDX_ID + "'");
                }
                consumer.accept(key, value);
            });
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Optional<V> put(String key, V value) {
        try {
            lock.writeLock().lock();

            final Map<String, Object> mapValue = currentObjectMapper().convertValue(value, JSON_MAP);
            final Document document = new Document(mapValue);
            document.put(IDX_ID, key);
            final Optional<V> previous = get(key);
            collection.insert(document);
            return previous;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public Optional<V> compute(final String key, final BiFunction<? super String, ? super Optional<V>, ? extends V> remappingFunction) {
        try {
            lock.writeLock().lock();

            final Optional<V> previous = get(key);
            final V computed = remappingFunction.apply(key, previous);
            if (previous.isPresent()) {
                if (computed != null) {
                    final Map<String, Object> mapValue = currentObjectMapper().convertValue(computed, JSON_MAP);
                    final Document document = new Document(mapValue);
                    document.put(IDX_ID, key);
                    collection.update(eq(IDX_ID, key), document);
                } else {
                    collection.remove(eq(IDX_ID, key));
                }
            } else {
                if (computed != null) {
                    final Map<String, Object> mapValue = currentObjectMapper().convertValue(computed, JSON_MAP);
                    final Document document = new Document(mapValue);
                    document.put(IDX_ID, key);
                    collection.insert(document);
                }
            }

            return Optional.ofNullable(computed);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public Optional<V> remove(String key) {
        try {
            lock.writeLock().lock();
            Optional<V> previous = get(key);
            collection.remove(eq(IDX_ID, key));
            return previous;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void clear() {
        try {
            lock.writeLock().lock();
            collection.remove(ALL);
        } finally {
          lock.writeLock().unlock();
        }
    }

    @Override
    public long size() {
        try {
            lock.readLock().lock();
            return collection.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void close() {
        try {
            lock.writeLock().lock();
            nitrite.close();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public static <V> NitriteStateRepository.Builder<V> builder(Class<V> clazz) {
        return new NitriteStateRepository.Builder<>(clazz);
    }

    public static final class Builder<V> {

        private final Class<V> clazz;
        private String name;
        private Set<String> indexedFields = newHashSet();
        private NitriteBuilder nitriteBuilder = Nitrite.builder();

        private Builder(Class<V> clazz) {
            this.clazz = clazz;
            this.name = clazz.getSimpleName();
        }

        public NitriteStateRepository.Builder<V> with(Function<NitriteBuilder, NitriteBuilder> func) {
            func.apply(nitriteBuilder);
            return this;
        }

        public NitriteStateRepository.Builder<V> withName(final String val) {
            name = val;
            return this;
        }

        public NitriteStateRepository.Builder<V> withIndexed(final String... fields) {
            return withIndexed(newHashSet(fields));
        }

        public NitriteStateRepository.Builder<V> withIndexed(final Set<String> fields) {
            this.indexedFields.addAll(fields);
            return this;
        }

        public NitriteStateRepository<V> build() {
            return new NitriteStateRepository<V>(name, clazz, indexedFields, nitriteBuilder);
        }
    }
}
