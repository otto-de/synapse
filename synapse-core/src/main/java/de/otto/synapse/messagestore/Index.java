package de.otto.synapse.messagestore;

import com.google.common.annotations.Beta;
import de.otto.synapse.message.Key;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * Identifies a single Index used by a {@link MessageStore} to index messages using some {@link Indexer}
 *
 * <p>Indexes could be used to access messages in the store that share some common properties. For example,
 * the {@link Index#PARTITION_KEY partition-key index} is used to retrieve all messages that
 * have the same partition key.</p>
 *
 * <p>You can introduce your own index by 1. introduce an instance of Index and 2. implement an {@link Indexer}
 * that is {@link Indexer#supports(Index) supporting} the new Index.</p>
 */
@Beta
public class Index {

    /**
     * Index that is used to access messages in a {@link MessageStore} by {@link MessageStoreEntry#getChannelName()}
     */
    public static final Index CHANNEL_NAME = new Index("channelName");
    /**
     * Index that is used to specify the origin of the {@link MessageStoreEntry}. Depending on the use-case, this
     * might be a simple hint like, for example, {@code "origin" : "Snapshot"}, or the name of the originating
     * service.
     */
    public static final Index ORIGIN = new Index("origin");
    /**
     * Index that is used to specify the service instance that has added the {@link MessageStoreEntry} to the
     * {@link MessageStore}. The value can be something like {@code hostname:port} or some other identfier
     * used to distinguish different instances of single service.
     */
    public static final Index SERVICE_INSTANCE = new Index("serviceInstance");
    /**
     * Index that is used to access messages in a {@link MessageStore} by {@link Key#partitionKey()}
     *
     * <p>The {@link #name()} of the index is {@code 'partitionKey'}</p>
     */
    public static final Index PARTITION_KEY = new Index("partitionKey");

    private final String fieldName;

    public Index(final String name) {
        this.fieldName = name;
    }

    public static Index valueOf(final String name) {
        return new Index(name);
    }

    /**
     * Returns the field name used to store the indexed value.
     *
     * <p>The {@code name} is used by {@link MessageStore} implementations to generate datastructures used
     * for the Index.</p>
     *
     * @return field name
     */
    @Nonnull
    public String name() {
        return fieldName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Index)) return false;
        Index that = (Index) o;
        return Objects.equals(fieldName, that.fieldName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldName);
    }

    @Override
    public String toString() {
        return "Index{" +
                "name='" + fieldName + '\'' +
                '}';
    }
}
