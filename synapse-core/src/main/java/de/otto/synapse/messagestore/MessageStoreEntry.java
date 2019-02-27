package de.otto.synapse.messagestore;

import de.otto.synapse.message.TextMessage;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.Objects;

public final class MessageStoreEntry implements Serializable {

    private final @Nonnull String channelName;
    private final @Nonnull TextMessage textMessage;

    private MessageStoreEntry(final @Nonnull String channelName,
                              final @Nonnull TextMessage textMessage) {
        this.channelName = channelName;
        this.textMessage = textMessage;
    }

    public static MessageStoreEntry of(final @Nonnull String channelName,
                                       final @Nonnull TextMessage textMessage) {
        return new MessageStoreEntry(channelName, textMessage);
    }

    @Nonnull
    public String getChannelName() {
        return channelName;
    }

    @Nonnull
    public TextMessage getTextMessage() {
        return textMessage;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MessageStoreEntry)) return false;
        MessageStoreEntry that = (MessageStoreEntry) o;
        return channelName.equals(that.channelName) &&
                textMessage.equals(that.textMessage);
    }

    @Override
    public int hashCode() {
        return Objects.hash(channelName, textMessage);
    }

    @Override
    public String toString() {
        return "MessageStoreEntry{" +
                "channelName='" + channelName + '\'' +
                ", textMessage=" + textMessage +
                '}';
    }
}
