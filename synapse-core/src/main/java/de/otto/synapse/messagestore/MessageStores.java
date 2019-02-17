package de.otto.synapse.messagestore;

import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.message.TextMessage;

import java.util.stream.Stream;

import static de.otto.synapse.channel.ChannelPosition.fromHorizon;

/**
 * Utilities and helpers used to create {@link MessageStore message stores}.
 */
public class MessageStores {

    private MessageStores() {
        /* do not instantiate this */
    }

    public static MessageStore emptyMessageStore() {

        return EMPTY_MESSAGE_STORE;
    }

    private static final MessageStore EMPTY_MESSAGE_STORE = new MessageStore() {

        @Override
        public ChannelPosition getLatestChannelPosition() {
            return fromHorizon();
        }

        @Override
        public Stream<TextMessage> stream() {
            return Stream.empty();
        }
    };
}
