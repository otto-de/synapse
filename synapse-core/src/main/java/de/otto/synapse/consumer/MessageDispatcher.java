package de.otto.synapse.consumer;

import de.otto.synapse.message.Message;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import static de.otto.synapse.message.Message.message;
import static de.otto.synapse.translator.ObjectMappers.currentObjectMapper;
import static java.util.Collections.synchronizedList;
import static java.util.Collections.unmodifiableList;
import static java.util.regex.Pattern.compile;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * A MessageConsumer that is acting as a Message Dispatcher for multiple {@link MessageConsumer message consumers}.
 * <p>
 *     <img src="http://www.enterpriseintegrationpatterns.com/img/MessageDispatcher.gif" alt="MessageDispatcher">
 * </p>
 * <p>
 *     As it is implementing the MessageConsumer interface, the MessageDispatcher is an implementation
 *     of the GoF Composite Pattern.
 * </p>
 * <p>
 *     Messages are translated by the dispatcher using to the format expected by the registered consumers.
 * </p>
 * @see  <a href="http://www.enterpriseintegrationpatterns.com/patterns/messaging/MessageDispatcher.html">EIP: Message Dispatcher</a>
 * @see <a href="https://en.wikipedia.org/wiki/Composite_pattern">Composite Pattern</a>
 */
public class MessageDispatcher implements MessageConsumer<String> {

    private static final Logger LOG = getLogger(MessageDispatcher.class);
    private static final Pattern ACCEPT_ALL = compile(".*");

    private final List<MessageConsumer<?>> messageConsumers;

    public MessageDispatcher() {
        this.messageConsumers = synchronizedList(new ArrayList<>());
    }

    public MessageDispatcher(final List<MessageConsumer<?>> messageConsumers) {
        this.messageConsumers = synchronizedList(new ArrayList<>(messageConsumers));
    }

    public void add(final MessageConsumer<?> messageConsumer) {
        this.messageConsumers.add(messageConsumer);
    }

    public List<MessageConsumer<?>> getAll() {
        return unmodifiableList(messageConsumers);
    }

    /**
     * Returns the expected payload type of {@link Message events} consumed by this EventConsumer.
     *
     * @return payload type
     */
    @Nonnull
    @Override
    public Class<String> payloadType() {
        return String.class;
    }

    /**
     * Returns the pattern of {@link Message#getKey() event keys} accepted by this consumer.
     *
     * @return Pattern
     */
    @Nonnull
    @Override
    public Pattern keyPattern() {
        return ACCEPT_ALL;
    }

    /**
     * Accepts a message with JSON String payload, dispatches this method to the different registered
     * {@link MessageConsumer consumers} if their {@link MessageConsumer#keyPattern()} matches, and
     * translates the JSON payload into the expected {@link MessageConsumer#payloadType()} of the receiving
     * MessageConsumer.
     *
     * @param message the input argument
     */
    @Override
    @SuppressWarnings({"unchecked", "raw"})
    public void accept(final Message<String> message) {
        LOG.debug("Accepting message={}", message);
        messageConsumers
                .stream()
                .filter(consumer -> matchesKeyPattern(message, consumer.keyPattern()))
                .forEach((MessageConsumer consumer) -> {
                    try {
                        final Class<?> payloadType = consumer.payloadType();
                        if (payloadType.equals(String.class)) {
                            consumer.accept(message);
                        } else {
                            Object payload = null;
                            if (message.getPayload() != null) {
                                payload = currentObjectMapper().readValue(message.getPayload(), payloadType);
                            }
                            final Message<?> tMessage = message(message.getKey(), message.getHeader(), payload);
                            consumer.accept(tMessage);
                        }
                    } catch (final Exception e) {
                        LOG.error(e.getMessage(), e);
                    }
                });
    }

    private boolean matchesKeyPattern(final Message<String> message,
                                      final Pattern keyPattern) {
        return keyPattern.matcher(message.getKey()).matches();
    }

}
