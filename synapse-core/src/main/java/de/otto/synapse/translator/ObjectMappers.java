package de.otto.synapse.translator;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;

/**
 * Manages the ObjectMapper used by Synapse applications for serialization and deserialization purposes.
 */

public final class ObjectMappers {

    private static final ObjectMapper DEFAULT_OBJECT_MAPPER;
    private static AtomicReference<ObjectMapper> SYNAPSE_OBJECT_MAPPER;

    static {
        DEFAULT_OBJECT_MAPPER = new ObjectMapper();
        DEFAULT_OBJECT_MAPPER.findAndRegisterModules();
        DEFAULT_OBJECT_MAPPER.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        DEFAULT_OBJECT_MAPPER.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        DEFAULT_OBJECT_MAPPER.disable(DeserializationFeature.FAIL_ON_INVALID_SUBTYPE);
        SYNAPSE_OBJECT_MAPPER = new AtomicReference<>(DEFAULT_OBJECT_MAPPER);
    }

    /**
     *
     * @return the default objectmapper configured as expected by Synapse.
     */
    public static ObjectMapper defaultObjectMapper() {
        return DEFAULT_OBJECT_MAPPER;
    }

    /**
     * Returns the {@code ObjectMapper} currently used by Synapse.
     *
     * <p>By default, this is the same as {@link #defaultObjectMapper()}. You can override this mapper using
     * {@link #overrideObjectMapper(ObjectMapper)}.</p>
     *
     * @return the {@code ObjectMapper} currently used by Synapse.
     */
    public static ObjectMapper currentObjectMapper() {
        return SYNAPSE_OBJECT_MAPPER.get();
    }

    /**
     * Overrides the object mapper actually used by Synapse.
     *
     * <p>Handle with care as there is a chance to break message
     * passing across Synapse services if configured in an unexpected way.</p>
     *
     * @param objectMapper the ObjectMapper used to serialize and deserialize messages
     */
    public static synchronized void overrideObjectMapper(final ObjectMapper objectMapper) {
        SYNAPSE_OBJECT_MAPPER.set(requireNonNull(objectMapper));
    }

    private ObjectMappers() {
    }
}
