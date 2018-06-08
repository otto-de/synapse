package de.otto.synapse.messagestore.aws;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.ShardPosition;
import de.otto.synapse.compaction.aws.SnapshotReadService;
import de.otto.synapse.info.SnapshotReaderNotification;
import de.otto.synapse.info.SnapshotReaderStatus;
import de.otto.synapse.message.Message;
import de.otto.synapse.messagestore.MessageStore;
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
import java.util.stream.Stream;
import java.util.zip.ZipInputStream;

import static com.google.common.collect.ImmutableMap.builder;
import static de.otto.synapse.channel.ChannelPosition.channelPosition;
import static de.otto.synapse.channel.ChannelPosition.fromHorizon;
import static de.otto.synapse.channel.ShardPosition.fromPosition;
import static de.otto.synapse.compaction.aws.SnapshotFileHelper.getSnapshotTimestamp;
import static de.otto.synapse.info.SnapshotReaderStatus.*;
import static de.otto.synapse.message.Header.responseHeader;
import static de.otto.synapse.message.Message.message;
import static org.slf4j.LoggerFactory.getLogger;

@NotThreadSafe
public class SnapshotMessageStore implements MessageStore {

    private static final Logger LOG = getLogger(SnapshotMessageStore.class);

    private MessageIterator messageIterator;
    private ChannelPosition channelPosition;
    private ZipInputStream zipInputStream;
    private Instant snapshotTimestamp;
    private final String channelName;
    private final ApplicationEventPublisher eventPublisher;

    public SnapshotMessageStore(final @Nonnull String channelName,
                                final @Nonnull SnapshotReadService snapshotReadService,
                                final @Nullable ApplicationEventPublisher eventPublisher) {
        this.channelName = channelName;
        this.eventPublisher = eventPublisher;
        publishEvent(STARTING, "Retrieve snapshot file from S3.", null);
        try {
        final Optional<File> latestSnapshot = snapshotReadService.retrieveLatestSnapshot(channelName);
            if (latestSnapshot.isPresent()) {
                final File snapshot = latestSnapshot.get();
                this.snapshotTimestamp = getSnapshotTimestamp(snapshot.getName());
                publishEvent(STARTED, "Retrieve snapshot file from S3.", snapshotTimestamp);
                    zipInputStream = new ZipInputStream(new BufferedInputStream(new FileInputStream(snapshot)));
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
                zipInputStream.close();
            } catch (final Exception e1) {
                /* ignore */
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
        } catch (final IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        }
    }

    @Override
    public ChannelPosition getLatestChannelPosition() {
        return channelPosition != null ? channelPosition : fromHorizon();
    }

    @Override
    public Stream<Message<String>> stream() {
        return messageIterator != null
                ? Streams.stream(messageIterator)
                : Stream.empty();
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
                        final ShardPosition shardPosition = sequenceNumber != null && !sequenceNumber.equals("")
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

    private static class MessageIterator implements Iterator<Message<String>> {

        private Message<String> nextMessage = null;
        private JsonParser jsonParser;

        private MessageIterator(final JsonParser jsonParser) {
            this.jsonParser = jsonParser;
        }

        @Override
        public boolean hasNext() {
            try {
                if (nextMessage == null) {
                    final Instant arrivalTimestamp = Instant.EPOCH;
                    while (!jsonParser.isClosed() && jsonParser.nextToken() != JsonToken.END_ARRAY) {
                        JsonToken currentToken = jsonParser.currentToken();
                        if (currentToken == JsonToken.FIELD_NAME) {
                            final String key = jsonParser.getValueAsString();
                            nextMessage = message(
                                    key,
                                    responseHeader(null, arrivalTimestamp),
                                    jsonParser.nextTextValue()
                            );
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
        public Message<String> next() {
            if (hasNext()) {
                final Message<String> nextMessage = this.nextMessage;
                this.nextMessage = null;
                return nextMessage;
            } else {
                throw new NoSuchElementException("No more messages available");
            }
        }
    }
}
