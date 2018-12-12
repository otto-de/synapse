package de.otto.synapse.translator;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.*;

public class ObjectMappersTest {

    @Test
    public void shouldOverrideObjectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectMappers.overrideObjectMapper(objectMapper);
        assertThat(ObjectMappers.currentObjectMapper(), is(objectMapper));
        assertThat(ObjectMappers.defaultObjectMapper(), is(not(objectMapper)));
    }
}