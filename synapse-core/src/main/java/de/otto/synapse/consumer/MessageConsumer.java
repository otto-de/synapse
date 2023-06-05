package de.otto.synapse.consumer;

import de.otto.synapse.configuration.SynapseProperties;
import de.otto.synapse.eventsource.EventSource;
import de.otto.synapse.eventsource.EventSourceConsumerProcess;
import de.otto.synapse.message.Message;
import jakarta.annotation.Nonnull;

import java.util.function.Consumer;
import java.util.regex.Pattern;

import static java.util.regex.Pattern.compile;

/**
 * A consumer endpoint for {@link Message messages} with payload-type &lt;T&gt;.
 * <p>
 *     <img src="http://www.enterpriseintegrationpatterns.com/img/MessageEndpointSolution.gif" alt="MesageConsumer">
 * </p>
 * <p>
 * Multiple EventConsumers may listen at a single {@link EventSource}. A single EventConsumer
 * must be registered to multiple EventSources.
 * </p>
 * <p>
 * By default, Synapse is auto-configuring a {@link EventSourceConsumerProcess} that is
 * running a separate thread for every MessageConsumer. The thread is taking care for continuous
 * consumption of messages using the consumers, until the application is shutting down.
 * </p>
 * <p>
 * If you need to manually consume messages using MessageConsumers, auto-configuration of the
 * {@code EventSourceConsumerProcess} can be disabled by setting
 * {@link SynapseProperties.ConsumerProcess#setEnabled(boolean) synapse.consumer-process.enabled=false}
 * </p>
 * <p>
 * MessageConsumers are expected to be thread-safe.
 * </p>
 *
 * @param <T> the type of the messages's payload
 */

public interface MessageConsumer<T> extends Consumer<Message<T>> {

    static <T> MessageConsumer<T> of(final String keyPattern,
                                     final Class<T> payloadType,
                                     final Consumer<Message<T>> consumer) {
        return new MessageConsumer<T>() {

            private Pattern pattern = compile(keyPattern);

            @Override
            @Nonnull
            public Class<T> payloadType() {
                return payloadType;
            }

            @Override
            @Nonnull
            public Pattern keyPattern() {
                return pattern;
            }

            @Override
            public void accept(Message<T> tMessage) {
                consumer.accept(tMessage);
            }
        };
    }

    /**
     * Returns the expected payload type of {@link Message events} consumed by this EventConsumer.
     *
     * @return payload type
     */
    @Nonnull
    Class<T> payloadType();

    /**
     * Returns the pattern of {@link de.otto.synapse.message.Message#getKey() message keys} accepted by this consumer.
     *
     * @return Pattern
     */
    @Nonnull
    Pattern keyPattern();

}
