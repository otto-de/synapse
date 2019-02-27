package de.otto.synapse.messagestore;

import com.google.common.collect.ImmutableSet;
import de.otto.synapse.channel.ChannelPosition;

import java.util.Set;
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
        public String getName() {
            return "nil";
        }

        @Override
        public Set<String> getChannelNames() {
            return ImmutableSet.of();
        }

        @Override
        public ChannelPosition getLatestChannelPosition(String channelName) {
            return ChannelPosition.fromHorizon();
        }

        @Override
        public ChannelPosition getLatestChannelPosition() {
            return fromHorizon();
        }

        @Override
        public Stream<MessageStoreEntry> streamAll() {
            return Stream.empty();
        }

        @Override
        public Stream<MessageStoreEntry> stream(String channelName) {
            return Stream.empty();
        }

        /**
         * Guaranteed to throw an exception and leave the message store unmodified.
         *
         * @throws UnsupportedOperationException always
         * @deprecated Unsupported operation.
         */
        @Deprecated
        @Override
        public void add(MessageStoreEntry entry) {
            throw new UnsupportedOperationException();
        }

    };
}
