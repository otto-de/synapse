package de.otto.synapse.translator;

import com.fasterxml.jackson.core.JsonProcessingException;

import java.io.IOException;
import java.util.Map;

import static de.otto.synapse.translator.ObjectMappers.currentObjectMapper;

/**
 * Helper-class for JSON representations.
 */
public class JsonHelper {

    /**
     * Transforms the given entity into a pretty-printed JSON representation.
     *
     * <p>The {@link ObjectMappers#currentObjectMapper()} is used to serialize the entity.</p>
     *
     * @param entity some json-serializable object
     * @return pretty-printed JSON, or "null" if entity is null.
     * @throws IllegalStateException if serializing the entity fails for some reason.
     */
    public static String prettyPrint(final Object entity) {
        if (entity == null) {
            return "null";
        }
        try {
            return currentObjectMapper()
                    .writerWithDefaultPrettyPrinter()
                    .writeValueAsString(entity);
        } catch (final JsonProcessingException e) {
            throw new IllegalStateException("Unable to transform entity to JSON: " + e.getMessage());
        }
    }

    /**
     * Pretty-prints the given json document into.
     *
     * @param json the json document
     * @return pretty-printed JSON.
     * @throws IllegalStateException if serializing the entity fails for some reason.
     */
    public static String prettyPrint(final String json) {
        try {
            return prettyPrint(currentObjectMapper().readValue(json, Map.class));
        } catch (final IOException e) {
            return json;
        }
    }

}
