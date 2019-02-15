package de.otto.synapse.translator;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;

/**
 * Manages the ObjectMapper used by Synapse applications for serialization and deserialization purposes.
 */
@ThreadSafe
public final class ObjectMappers {

    private static final ObjectMapper DEFAULT_OBJECT_MAPPER;
    private static AtomicReference<ObjectMapper> SYNAPSE_OBJECT_MAPPER;

    static {
        DEFAULT_OBJECT_MAPPER = new ObjectMapper();
        DEFAULT_OBJECT_MAPPER.findAndRegisterModules();
        DEFAULT_OBJECT_MAPPER.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
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
     *
     * @return the objectmapper actually used by Synapse. By default, this is the same as {@link #defaultObjectMapper()}
     */
    public static ObjectMapper currentObjectMapper() {
        return SYNAPSE_OBJECT_MAPPER.get();
    }

    /**
     * Overrides the object mapper actually used by Synapse. Handle with care as there is a chance to break message
     * passing across Synapse services if configured in an unexpected way.
     *
     * @param objectMapper the ObjectMapper used to serialize and deserialize messages
     */
    public static synchronized void overrideObjectMapper(final ObjectMapper objectMapper) {
        SYNAPSE_OBJECT_MAPPER.set(requireNonNull(objectMapper));
    }

    private ObjectMappers() {
    }
}
