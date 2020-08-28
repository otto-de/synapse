package de.otto.synapse.messagestore.aws;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.ShardPosition;
import de.otto.synapse.compaction.s3.SnapshotFileHelper;
import de.otto.synapse.compaction.s3.SnapshotMessage;
import de.otto.synapse.compaction.s3.SnapshotMessageDecoder;
import de.otto.synapse.compaction.s3.SnapshotReadService;
import de.otto.synapse.info.SnapshotReaderNotification;
import de.otto.synapse.info.SnapshotReaderStatus;
import de.otto.synapse.message.Header;
import de.otto.synapse.message.Key;
import de.otto.synapse.message.TextMessage;
import de.otto.synapse.messagestore.Index;
import de.otto.synapse.messagestore.MessageStoreEntry;
import de.otto.synapse.messagestore.SnapshotMessageStore;
import de.otto.synapse.translator.Decoder;
import org.slf4j.Logger;
import org.springframework.context.ApplicationEventPublisher;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.io.*;
import java.time.Instant;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import java.util.zip.ZipInputStream;

import static com.google.common.collect.ImmutableMap.builder;
import static de.otto.synapse.channel.ChannelPosition.channelPosition;
import static de.otto.synapse.channel.ChannelPosition.fromHorizon;
import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static de.otto.synapse.info.SnapshotReaderStatus.*;
import static org.slf4j.LoggerFactory.getLogger;

@NotThreadSafe
public class S3SnapshotMessageStore implements SnapshotMessageStore {

    private static final Logger LOG = getLogger(S3SnapshotMessageStore.class);

    private MessageIterator messageIterator;
    private ChannelPosition channelPosition;

    private ZipInputStream zipInputStream;
    private BufferedInputStream bufferedInputStream;
    private FileInputStream fileInputStream;

    private Instant snapshotTimestamp;
    private final String channelName;
    private final ApplicationEventPublisher eventPublisher;

    public S3SnapshotMessageStore(final @Nonnull String channelName,
                                  final @Nonnull SnapshotReadService snapshotReadService,
                                  final @Nullable ApplicationEventPublisher eventPublisher) {
        this.channelName = channelName;
        this.eventPublisher = eventPublisher;
        publishEvent(STARTING, "Retrieve snapshot file from S3.", null);
        try {
            final Optional<File> latestSnapshot = snapshotReadService.retrieveLatestSnapshot(channelName);
            if (latestSnapshot.isPresent()) {
                final File snapshotFile = latestSnapshot.get();
                this.snapshotTimestamp = SnapshotFileHelper.getSnapshotTimestamp(snapshotFile.getName());
                publishEvent(STARTED, "Retrieve snapshot file from S3.", snapshotTimestamp);
                fileInputStream = new FileInputStream(snapshotFile);
                bufferedInputStream = new BufferedInputStream(fileInputStream);
                zipInputStream = new ZipInputStream(bufferedInputStream);
                zipInputStream.getNextEntry();
                JsonFactory jsonFactory = new JsonFactory();
                final JsonParser jsonParser = jsonFactory.createParser(zipInputStream);
                while (!jsonParser.isClosed() && messageIterator == null) {
                    JsonToken currentToken = jsonParser.nextToken();
                    if (currentToken == JsonToken.FIELD_NAME) {
                        switch (jsonParser.getValueAsString()) {
                            case "startSequenceNumbers":
                                channelPosition = processSequenceNumbers(jsonParser);
                                break;
                            case "data":
                                // TODO: This expects "startSequenceNumbers" to come _before_ "data"
                                messageIterator = new MessageIterator(jsonParser);
                                break;
                            default:
                                break;
                        }
                    }
                }
            } else {
                LOG.info("No Snapshot available. Returning emptyMessageStore MessageStore.");
            }
        } catch (final Exception e) {
            try {
                if (zipInputStream != null) {
                    zipInputStream.close();
                }
            } catch (final Exception e1) {
                LOG.error("Error closing zipInputStream", e1);
            }
            try {
                if (bufferedInputStream != null) {
                    bufferedInputStream.close();
                }
            } catch (IOException ioException) {
                LOG.error("Exception closing bufferedInputStream", ioException);
            }
            try {
                if (fileInputStream != null) {
                    fileInputStream.close();
                }
            } catch (IOException ioException) {
                LOG.error("Exception closing fileInputStream", ioException);
            }
            publishEvent(FAILED, "Failed to load snapshot from S3: " + e.getMessage(), snapshotTimestamp);
            throw new RuntimeException(e);
        }

    }

    public void close() {
        LOG.info("Closing SnapshotMessageStore");
        publishEvent(FINISHED, "Finished to load snapshot from S3.", snapshotTimestamp);
        try {
            if (zipInputStream != null) {
                zipInputStream.close();
            }
            if (bufferedInputStream != null) {
                bufferedInputStream.close();
            }
            if (fileInputStream != null) {
                fileInputStream.close();
            }
        } catch (final IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        }
    }

    @Override
    public Instant getSnapshotTimestamp() {
        return snapshotTimestamp;
    }

    @Override
    public Set<String> getChannelNames() {
        return ImmutableSet.of(channelName);
    }

    @Override
    public ImmutableSet<Index> getIndexes() {
        return ImmutableSet.of();
    }

    @Override
    public ChannelPosition getLatestChannelPosition(String channelName) {
        return channelName.equals(this.channelName)
                ? getLatestChannelPosition()
                : fromHorizon();
    }

    @Override
    public ChannelPosition getLatestChannelPosition() {
        return channelPosition != null
                ? channelPosition
                : fromHorizon();
    }

    @Override
    public Stream<MessageStoreEntry> stream() {
        return messageIterator != null
                ? Streams.stream(messageIterator).map(msg -> MessageStoreEntry.of(channelName, ImmutableMap.of(Index.ORIGIN, "Snapshot"), msg))
                : Stream.empty();
    }

    /**
     * Guaranteed to throw an exception and leave the message store unmodified.
     *
     * @throws UnsupportedOperationException always
     * @deprecated Unsupported operation.
     */
    @Override
    public Stream<MessageStoreEntry> stream(Index index, String value) {
        throw new UnsupportedOperationException();
    }

    /**
     * Guaranteed to throw an exception and leave the message store unmodified.
     *
     * @throws UnsupportedOperationException always
     * @deprecated Unsupported operation.
     */
    @Deprecated
    @Override
    public void add(@Nonnull MessageStoreEntry entry) {
        throw new UnsupportedOperationException();
    }
    
    private ChannelPosition processSequenceNumbers(final JsonParser parser) throws IOException {
        final ImmutableMap.Builder<String, ShardPosition> shardPositions = builder();

        String shardName = null;
        String sequenceNumber = null;
        while (parser.nextToken() != JsonToken.END_ARRAY) {
            JsonToken currentToken = parser.currentToken();
            switch (currentToken) {
                case FIELD_NAME:
                    switch (parser.getValueAsString()) {
                        case "shard":
                            parser.nextToken();
                            shardName = parser.getValueAsString();
                            break;
                        case "sequenceNumber":
                            parser.nextToken();
                            sequenceNumber = parser.getValueAsString();
                            break;
                        default:
                            break;
                    }
                    break;
                case END_OBJECT:
                    if (shardName != null) {
                        final ShardPosition shardPosition = ((sequenceNumber != null) && !sequenceNumber.equals("") && !sequenceNumber.equals("0"))
                                ? fromPosition(shardName, sequenceNumber)
                                : ShardPosition.fromHorizon(shardName);
                        shardPositions.put(shardName, shardPosition);
                    }
                    shardName = null;
                    sequenceNumber = null;
                    break;
                default:
                    break;
            }
        }
        return channelPosition(shardPositions.build().values());
    }

    private void publishEvent(final SnapshotReaderStatus status,
                              final String message,
                              final Instant snapshotTimestamp) {
        if (eventPublisher != null) {
            SnapshotReaderNotification notification = SnapshotReaderNotification.builder()
                    .withSnapshotTimestamp(snapshotTimestamp)
                    .withChannelName(channelName)
                    .withStatus(status)
                    .withMessage(message)
                    .build();
            try {
                eventPublisher.publishEvent(notification);
            } catch (Exception e) {
                LOG.error("error publishing event source notification: {}", notification, e);
            }
        }
    }

    @Override
    public boolean isCompacting() {
        return true;
    }

    private static class MessageIterator implements Iterator<TextMessage> {

        private TextMessage nextMessage = null;
        private JsonParser jsonParser;
        private final Decoder<SnapshotMessage> decoder = new SnapshotMessageDecoder();

        private MessageIterator(final JsonParser jsonParser) {
            this.jsonParser = jsonParser;
        }

        @Override
        public boolean hasNext() {
            try {
                if (nextMessage == null) {
                    while (!jsonParser.isClosed() && jsonParser.nextToken() != JsonToken.END_ARRAY) {
                        JsonToken currentToken = jsonParser.currentToken();
                        if (currentToken == JsonToken.FIELD_NAME) {
                            nextMessage = decoder.apply(new SnapshotMessage(
                                    Key.of(jsonParser.getValueAsString()),
                                    Header.of(),
                                    jsonParser.nextTextValue()));
                            break;
                        }
                    }
                }
                return nextMessage != null;
            } catch (final IOException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }

        @Override
        public TextMessage next() {
            if (hasNext()) {
                final TextMessage nextMessage = this.nextMessage;
                this.nextMessage = null;
                return nextMessage;
            } else {
                throw new NoSuchElementException("No more messages available");
            }
        }
    }
}
