package de.otto.synapse.translator;

import de.otto.synapse.message.Message;
import de.otto.synapse.message.TextMessage;

import java.util.function.Function;

/**
 * An {@code Encoder} IS-A {@code Function} that is encoding a text message into some target type,
 * mostly used as an adapter to some sender-endpoint infrastructure.
 *
 * @param <T> the target type of the encoder
 */
public interface Encoder<T> extends Function<Message<String>, T> {
}
