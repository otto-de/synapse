package de.otto.synapse.translator;

import de.otto.synapse.message.TextMessage;

import java.util.function.Function;

/**
 * A {@code Decoder} IS-A {@code Function} that is decoding objects of Type &lt;T&gt; into
 * {@link TextMessage TextMessages}, mostly used as an adapter to some receiver-endpoint
 * infrastructure.
 *
 * @param <T> the type of the source objects that are decoded into text messages.
 */
public interface Decoder<T> extends Function<T, TextMessage> {
}
