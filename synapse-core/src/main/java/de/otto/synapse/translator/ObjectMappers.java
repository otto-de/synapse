package de.otto.synapse.translator;

import com.fasterxml.jackson.databind.ObjectMapper;

import static com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS;

public class ObjectMappers {

    private static final ObjectMapper OBJECT_MAPPER;

    static {
        OBJECT_MAPPER = new ObjectMapper();
        OBJECT_MAPPER.findAndRegisterModules();
        OBJECT_MAPPER.disable(WRITE_DATES_AS_TIMESTAMPS);
    }

    public static ObjectMapper defaultObjectMapper() {
        return OBJECT_MAPPER;
    }

}
