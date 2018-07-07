package de.otto.synapse.endpoint.receiver.aws;

import de.otto.synapse.channel.ChannelDurationBehind;
import de.otto.synapse.channel.ChannelPosition;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.consumer.MessageDispatcher;
import de.otto.synapse.endpoint.InterceptorChain;
import de.otto.synapse.message.Message;
import de.otto.synapse.translator.MessageTranslator;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.collect.Maps.uniqueIndex;
import static de.otto.synapse.channel.ChannelDurationBehind.channelDurationBehind;
import static de.otto.synapse.channel.ChannelPosition.channelPosition;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class KinesisMessageLogResponse {

    private final String channelName;
    private final Map<String, KinesisShardResponse> shardResponses;

    public KinesisMessageLogResponse(final List<KinesisShardResponse> shardResponses) {
        if (shardResponses.isEmpty()) {
            throw new IllegalArgumentException("Unable to create KinesisMessageLogResponse without KinesisShardResponses");
        }
        if (shardResponses.stream().map(KinesisShardResponse::getChannelName).distinct().count() > 1) {
            throw new IllegalArgumentException("Unable to create KinesisMessageLogResponse from KinesisShardResponses returned by different message channels");
        }
        this.channelName = shardResponses.get(0).getChannelName();
        this.shardResponses = uniqueIndex(shardResponses, KinesisShardResponse::getShardName);
    }

    public String getChannelName() {
        return channelName;
    }

    public ChannelDurationBehind getChannelDurationBehind() {
        final ChannelDurationBehind.Builder durationBehind = channelDurationBehind();
        shardResponses.forEach((key, value) -> durationBehind.with(key, value.getDurationBehind()));
        return durationBehind.build();
    }

    public List<Message<String>> getMessages() {
        return shardResponses
                .values()
                .stream()
                .flatMap(response -> response.getMessages().stream())
                .collect(toList());
    }

    /**
     * Translates the messages from the response into messages with {@link Message#getPayload() payload} of type
     * &lt;P&gt; and returns the translated messages as a list.
     *
     * @param messageTranslator the {@link MessageTranslator} used to translate the message payload
     * @param <P> the type of the returned messages payload
     * @return list of messages with payload type &lt;P&gt;
     */
    public <P> List<Message<P>> getMessages(final MessageTranslator<P> messageTranslator) {
        return shardResponses
                .values()
                .stream()
                .flatMap(response -> response.getMessages().stream().map(messageTranslator::translate))
                .collect(toList());
    }

    /**
     * Dispatches all messages from the response to some {@link MessageConsumer}.
     * <p>
     *     The {@link MessageDispatcher} can be used to consume and transform the messages into messages
     *     with some custom payload and dispatch these messages to one or more {@code MessageConsumers}:
     * </p>
     * <pre><code>
     *     final KinesisMessageLogResponse response = kinesisMessageLogReader.read(iterator).get();
     *     final MessageConsumer&lt;TestPayload&gt; consumer = someTestMessageConsumer();
     *     response.dispatchMessages(new MessageDispatcher(new ObjectMapper(), singletonList(consumer)));
     * </code></pre>
     * @param messageConsumer the MessageConsumer
     */
    public void dispatchMessages(final MessageConsumer<String> messageConsumer) {
        shardResponses
                .values()
                .stream()
                .flatMap(response -> response.getMessages().stream())
                .forEach(messageConsumer);
    }

    public Set<String> getShardNames() {
        return shardResponses
                .values()
                .stream()
                .map(KinesisShardResponse::getShardName)
                .collect(toSet());
    }

    public Collection<KinesisShardResponse> getShardResponses() {
        return shardResponses.values();
    }

    public ChannelPosition getChannelPosition() {
        return channelPosition(shardResponses.values()
                .stream()
                .map(KinesisShardResponse::getShardPosition)
                .collect(Collectors.toList()));
    }
}
