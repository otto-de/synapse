package de.otto.synapse.translator;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;


public class MessageFormatTest {

    @Test
    public void shouldReturnDefaultMessageFormat() {
        assertThat(MessageFormat.defaultMessageFormat()).isEqualTo(MessageFormat.V1);
    }
}