package de.otto.synapse.endpoint.sender.kafka;

import com.google.common.collect.ImmutableList;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.sender.AbstractMessageSenderEndpoint;
import de.otto.synapse.message.TextMessage;
import de.otto.synapse.translator.MessageFormat;
import de.otto.synapse.translator.MessageTranslator;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.springframework.kafka.core.KafkaTemplate;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.CompletableFuture.allOf;
import static org.apache.kafka.common.utils.Utils.murmur2;
import static org.apache.kafka.common.utils.Utils.toPositive;
import static org.slf4j.LoggerFactory.getLogger;

public class KafkaMessageSender extends AbstractMessageSenderEndpoint {

    private static final Logger LOG = getLogger(KafkaMessageSender.class);

    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaMessageSender(final String channelName,
                              final MessageInterceptorRegistry interceptorRegistry,
                              final MessageTranslator<TextMessage> messageTranslator,
                              final KafkaTemplate<String, String> kafkaTemplate) {
        super(channelName, interceptorRegistry, messageTranslator);
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    protected CompletableFuture<Void> doSend(@Nonnull TextMessage message) {
        // TODO: Introduce a response object and return it instead of Void

        if (message.getKey().isCompoundKey()) {
            final String partitionKey = message.getKey().partitionKey();
            final int partition = kafkaPartitionFrom(partitionKey);
            // Just because we need a CompletableFuture<Void>, no CompletableFuture<SendMessageBatchResponse>:
            return allOf(kafkaTemplate
                    .send(new ProducerRecord<>(
                            getChannelName(),
                            partition,
                            message.getKey().compactionKey(),
                            message.getPayload(),
                            headersOf(message)
                    ))
                    .completable());
        } else {
            return allOf(kafkaTemplate
                    .send(new ProducerRecord<>(
                            getChannelName(),
                            null,
                            message.getKey().compactionKey(),
                            message.getPayload(),
                            headersOf(message)))
                    .completable());
        }

    }

    private List<org.apache.kafka.common.header.Header> headersOf(final TextMessage message) {
        final ImmutableList.Builder<org.apache.kafka.common.header.Header> messageAttributes = ImmutableList.builder();
        message.getHeader().getAll().entrySet().forEach(entry -> {
            messageAttributes.add(new RecordHeader(entry.getKey(), entry.getValue().getBytes(UTF_8)));
        });
        return messageAttributes.build();
    }

    @Override
    protected CompletableFuture<Void> doSendBatch(@Nonnull Stream<TextMessage> messageStream) {
        return allOf(messageStream.map(this::doSend).toArray(CompletableFuture[]::new));
    }

    private int kafkaPartitionFrom(String partitionKey) {
        final int numPartitions = kafkaTemplate.partitionsFor(getChannelName()).size();
        return toPositive(murmur2(partitionKey.getBytes())) % numPartitions;
    }

    @Override
    public MessageFormat getMessageFormat() {
        return MessageFormat.V1;
    }

}
