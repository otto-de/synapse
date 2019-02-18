package de.otto.synapse.translator;

import de.otto.synapse.message.Header;
import de.otto.synapse.message.Key;
import de.otto.synapse.message.TextMessage;
import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;

public final class TextDecoder extends AbstractTextDecoder<String> {

    private static final Logger LOG = getLogger(TextDecoder.class);

    @Override
    public TextMessage apply(final String s) {
        return decode(Key.of(), Header.of(), s);
    }
}
