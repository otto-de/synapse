package de.otto.synapse.edison.history;

import de.otto.synapse.message.Message;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

import static de.otto.synapse.message.DefaultHeaderAttr.MSG_RECEIVER_TS;
import static de.otto.synapse.message.DefaultHeaderAttr.MSG_SENDER_TS;

/**
 * An entry in the {@link History} of some entity in a {@link de.otto.synapse.state.StateRepository}.
 * <p>
 *     A {@code HistoryEntry} consists of a single message, together with the name of the channel and a
 *     list of {@link Diff diffs}
 * </p>
 */
public class HistoryEntry {

    private final String messageKey;
    private final Object messagePayload;
    private final Instant senderTimestamp;
    private final Instant receiverTimestamp;
    private final String channelName;
    private final List<Diff> diffs;

    public HistoryEntry(final Message<?> message,
                        final String channelName,
                        final List<Diff> diffs) {
        this.messageKey = message.getKey().partitionKey();
        this.messagePayload = message.getPayload();
        this.senderTimestamp = message.getHeader().getAsInstant(MSG_SENDER_TS.key());
        this.receiverTimestamp = message.getHeader().getAsInstant(MSG_RECEIVER_TS.key());
        this.channelName = channelName;
        this.diffs = diffs;
    }

    public String getMessageKey() {
        return messageKey;
    }

    public Object getMessagePayload() {
        return messagePayload;
    }

    public Instant getSenderTimestamp() {
        return senderTimestamp;
    }

    public Instant getReceiverTimestamp() {
        return receiverTimestamp;
    }

    public String getChannelName() {
        return channelName;
    }

    public List<Diff> getDiffs() {
        return diffs;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HistoryEntry that = (HistoryEntry) o;
        return Objects.equals(messageKey, that.messageKey) &&
                Objects.equals(messagePayload, that.messagePayload) &&
                Objects.equals(senderTimestamp, that.senderTimestamp) &&
                Objects.equals(receiverTimestamp, that.receiverTimestamp) &&
                Objects.equals(channelName, that.channelName) &&
                Objects.equals(diffs, that.diffs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(messageKey, messagePayload, senderTimestamp, receiverTimestamp, channelName, diffs);
    }

    @Override
    public String toString() {
        return "HistoryEntry{" +
                "messageKey='" + messageKey + '\'' +
                ", messagePayload=" + messagePayload +
                ", senderTimestamp=" + senderTimestamp +
                ", receiverTimestamp=" + receiverTimestamp +
                ", channelName='" + channelName + '\'' +
                ", diffs=" + diffs +
                '}';
    }
}
