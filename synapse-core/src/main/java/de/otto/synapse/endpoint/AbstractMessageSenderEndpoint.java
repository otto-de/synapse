package de.otto.synapse.endpoint;

import de.otto.synapse.message.Message;
import de.otto.synapse.translator.MessageTranslator;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * Abstract implementation of a MessageSenderEndpoint with support for {@link MessageTranslator message translation}.
 * and {@link de.otto.synapse.endpoint.MessageInterceptor interception}.
 */
public abstract class AbstractMessageSenderEndpoint extends AbstractMessageEndpoint implements MessageSenderEndpoint {

    private final MessageTranslator<String> messageTranslator;

    public AbstractMessageSenderEndpoint(final String channelName,
                                         final MessageTranslator<String> messageTranslator) {
        super(channelName);
        this.messageTranslator = messageTranslator;
    }

    public AbstractMessageSenderEndpoint(final String channelName,
                                         final MessageTranslator<String> messageTranslator,
                                         final MessageInterceptor interceptor) {
        super(channelName, interceptor);
        this.messageTranslator = messageTranslator;
    }

    @Override
    public final <T> void send(final Message<T> message) {
        final Message<String> translatedMessage = messageTranslator.translate(message);
        final Message<String> interceptedMessage = intercept(translatedMessage);
        if (interceptedMessage != null) {
            doSend(interceptedMessage);
        }
    }

    @Override
    public final <T> void sendBatch(final Stream<Message<T>> batch) {
        doSendBatch(batch
                .map(messageTranslator::translate)
                .map(this::intercept)
                .filter(Objects::nonNull));
    }

    protected void doSendBatch(final @Nonnull Stream<Message<String>> batch) {
        batch.forEach(this::doSend);
    }

    protected abstract void doSend(final @Nonnull Message<String> message);

}
