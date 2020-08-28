package de.otto.synapse.compaction.s3;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.collect.ImmutableMap;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.channel.ShardPosition;
import de.otto.synapse.consumer.MessageDispatcher;
import de.otto.synapse.message.Header;
import de.otto.synapse.message.Key;
import de.otto.synapse.message.TextMessage;
import de.otto.synapse.translator.Decoder;

import java.io.*;
import java.util.zip.ZipInputStream;

import static com.google.common.collect.ImmutableMap.builder;
import static de.otto.synapse.channel.ChannelPosition.channelPosition;
import static de.otto.synapse.channel.ChannelPosition.fromHorizon;
import static de.otto.synapse.channel.ShardPosition.fromHorizon;
import static de.otto.synapse.channel.ShardPosition.fromPosition;

public class SnapshotParser {

    private final JsonFactory jsonFactory = new JsonFactory();

    public ChannelPosition parse(final File latestSnapshot,
                                 final MessageDispatcher messageDispatcher) {

        try (
                FileInputStream fileInputStream = new FileInputStream(latestSnapshot);
                BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
                ZipInputStream zipInputStream = new ZipInputStream(bufferedInputStream)
        ) {
            ChannelPosition channelPosition = fromHorizon();
            zipInputStream.getNextEntry();
            JsonParser parser = jsonFactory.createParser(zipInputStream);
            while (!parser.isClosed()) {
                JsonToken currentToken = parser.nextToken();
                if (currentToken == JsonToken.FIELD_NAME) {
                    switch (parser.getValueAsString()) {
                        case "startSequenceNumbers":
                            channelPosition = processSequenceNumbers(parser);
                            break;
                        case "data":
                            // TODO: This expects "startSequenceNumbers" to come _before_ "data" which can/should not be guaranteed in JSON
                            processSnapshotData(
                                    parser,

                                    /* TODO: Hier wird der StreamName als ShardName verwendet. Damit wird der Message _keine_ sinnvolle Position im Header mitgegeben! */

                                    channelPosition, /*shardPositions.shard(channelName).position(),*/
                                    messageDispatcher);
                            break;
                        default:
                            break;
                    }
                }
            }
            return channelPosition;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private <T> void processSnapshotData(final JsonParser parser,
                                         final ChannelPosition channelPosition,
                                         final MessageDispatcher messageDispatcher) throws IOException {
        final Decoder<SnapshotMessage> decoder = new SnapshotMessageDecoder();
        final ShardPosition shardPosition = channelPosition.shard(channelPosition.shards().iterator().next());
        while (parser.nextToken() != JsonToken.END_ARRAY) {
            JsonToken currentToken = parser.currentToken();
            if (currentToken == JsonToken.FIELD_NAME) {
                final TextMessage message = decoder.apply(new SnapshotMessage(
                        Key.of(parser.getValueAsString()),
                        Header.of(shardPosition),
                        parser.nextTextValue()));
                messageDispatcher.accept(message);
            }
        }
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
                        // TODO: "0" kann entfernt werden, wenn keine Snapshots mit "0" für HORIZON mehr exisiteren.
                        final ShardPosition shardPosition = sequenceNumber != null && !sequenceNumber.equals("0") && !sequenceNumber.equals("")
                                ? fromPosition(shardName, sequenceNumber)
                                : fromHorizon(shardName);
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


}
