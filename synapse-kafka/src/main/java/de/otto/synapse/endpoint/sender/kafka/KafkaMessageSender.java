package de.otto.synapse.endpoint.sender.kafka;

import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.sender.AbstractMessageSenderEndpoint;
import de.otto.synapse.message.TextMessage;
import de.otto.synapse.translator.MessageFormat;
import de.otto.synapse.translator.MessageTranslator;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static java.util.concurrent.CompletableFuture.allOf;
import static org.slf4j.LoggerFactory.getLogger;

public class KafkaMessageSender extends AbstractMessageSenderEndpoint {

    private static final Logger LOG = getLogger(KafkaMessageSender.class);
    public static final long UPDATE_PARTITION_DELAY = 10_000L;

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final AtomicReference<KafkaEncoder> encoder;

    public KafkaMessageSender(final String channelName,
                              final MessageInterceptorRegistry interceptorRegistry,
                              final MessageTranslator<TextMessage> messageTranslator,
                              final KafkaTemplate<String, String> kafkaTemplate) {
        super(channelName, interceptorRegistry, messageTranslator);
        this.kafkaTemplate = kafkaTemplate;
        this.encoder = new AtomicReference<>(encoder());
    }

    @Scheduled(initialDelay = UPDATE_PARTITION_DELAY, fixedDelay = UPDATE_PARTITION_DELAY)
    public void updatePartitions() {
        encoder.set(encoder());
    }

    @Override
    protected CompletableFuture<Void> doSend(@Nonnull TextMessage message) {
        // TODO: Introduce a response object and return it instead of Void
        final ProducerRecord<String, String> record = encoder.get().apply(message);
        // Just because we need a CompletableFuture<Void>, no CompletableFuture<SendMessageBatchResponse>:
        return allOf(kafkaTemplate
                .send(record)
                .completable());
    }

    @Override
    protected CompletableFuture<Void> doSendBatch(@Nonnull Stream<TextMessage> messageStream) {
        return allOf(messageStream.map(this::doSend).toArray(CompletableFuture[]::new));
    }

    @Override
    public MessageFormat getMessageFormat() {
        return MessageFormat.V1;
    }

    private KafkaEncoder encoder() {
        final int numPartitions = kafkaTemplate.partitionsFor(getChannelName()).size();
        return new KafkaEncoder(getChannelName(), numPartitions);
    }
}
