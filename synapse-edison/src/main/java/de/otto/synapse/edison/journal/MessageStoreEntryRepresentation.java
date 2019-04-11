package de.otto.synapse.edison.journal;

import de.otto.synapse.message.Header;
import de.otto.synapse.messagestore.MessageStoreEntry;

import java.util.Map;
import java.util.Objects;

import static de.otto.synapse.translator.ObjectMappers.currentObjectMapper;

public class MessageStoreEntryRepresentation {

    private final String key;
    private final Header header;
    private final Object payload;
    private final String channelName;

    public MessageStoreEntryRepresentation(final MessageStoreEntry entry) {
        this.channelName = entry.getChannelName();
        this.key = entry.getTextMessage().getKey().toString();
        this.header = entry.getTextMessage().getHeader();
        Object p;
        try {
            p = currentObjectMapper()
                    .readValue(entry.getTextMessage().getPayload(), Map.class);
        } catch (final Exception e) {
            p = entry.getTextMessage().getPayload();
        }
        this.payload = p;
    }

    public String getKey() {
        return key;
    }

    public Header getHeader() {
        return header;
    }

    public Object getPayload() {
        return payload;
    }

    public String getChannelName() {
        return channelName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MessageStoreEntryRepresentation)) return false;
        MessageStoreEntryRepresentation that = (MessageStoreEntryRepresentation) o;
        return Objects.equals(key, that.key) &&
                Objects.equals(header, that.header) &&
                Objects.equals(payload, that.payload) &&
                Objects.equals(channelName, that.channelName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, header, payload, channelName);
    }

    @Override
    public String toString() {
        return "MessageStoreEntryRepresentation{" +
                "key='" + key + '\'' +
                ", header=" + header +
                ", payload=" + payload +
                ", channelName='" + channelName + '\'' +
                '}';
    }
}
